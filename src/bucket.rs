use crate::contact::{Contact, ContactRef};
use crate::id::NodeId;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq)]
pub enum BucketResult {
    Fail,
    Success,
    RequireSplit,
}

#[derive(Debug)]
pub struct Bucket {
    pub live: Vec<Contact>,
    pub extra: Vec<Contact>,
    pub last_updated: Instant,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            live: vec![],
            extra: vec![],
            last_updated: Instant::now(),
        }
    }
}

impl Bucket {
    // The 'K' constant in Kademlia algorithm
    pub const MAX_LEN: usize = 8;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_full(&self) -> bool {
        self.live.len() >= Self::MAX_LEN && self.extra.len() >= Self::MAX_LEN
    }

    pub fn get_contacts<'a>(&'a self, out: &mut Vec<ContactRef<'a>>) {
        self.live
            .iter()
            .filter(|c| !c.failed())
            .for_each(|c| out.push(c.as_ref()));
    }

    pub fn need_refresh(&self) -> bool {
        // Refresh every 15 mins
        const REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);

        self.last_updated < Instant::now() - REFRESH_INTERVAL
    }

    pub fn replace_node(&mut self, contact: &Contact) -> BucketResult {
        debug_assert!(self.live.len() >= Bucket::MAX_LEN);

        if replace_stale(&mut self.live, contact) || replace_stale(&mut self.extra, contact) {
            self.last_updated = Instant::now();
            BucketResult::Success
        } else {
            BucketResult::RequireSplit
        }
    }

    pub fn split(&mut self, own_id: &NodeId, curr_index: usize) -> Bucket {
        debug_assert!(self.live.len() >= Bucket::MAX_LEN);

        let mut new_bucket = Bucket::new();
        let mut i = 0;
        while i < self.live.len() {
            let bucket_index = self.live[i].id.xlz(own_id);
            if bucket_index == curr_index {
                i += 1;
                continue;
            }

            new_bucket.live.push(self.live.remove(i));
        }

        if self.live.len() > Bucket::MAX_LEN {
            self.extra.extend(self.live.drain(Bucket::MAX_LEN..));
        }

        let mut i = 0;
        while i < self.extra.len() {
            let bucket_index = self.extra[i].id.xlz(own_id);
            if bucket_index == curr_index {
                if self.live.len() >= Bucket::MAX_LEN {
                    i += 1;
                    continue;
                }
                self.live.push(self.extra.remove(i));
            } else {
                let contact = self.extra.remove(i);
                if new_bucket.live.len() < Bucket::MAX_LEN {
                    new_bucket.live.push(contact);
                } else {
                    new_bucket.extra.push(contact);
                }
            }
        }

        new_bucket
    }
}

fn replace_stale(vec: &mut Vec<Contact>, contact: &Contact) -> bool {
    if let Some(most_stale) = vec.iter_mut().max_by_key(|c| c.fail_count()) {
        if most_stale.fail_count() > 0 {
            *most_stale = contact.clone();
            return true;
        }
    }
    false
}
