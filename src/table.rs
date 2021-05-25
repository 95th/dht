use crate::contact::{Contact, ContactRef, ContactStatus};
use crate::id::NodeId;
use crate::util::to_ipv6;
use crate::{
    bucket::{Bucket, BucketResult},
    server::ClientRequest,
};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Instant;

#[derive(Debug)]
pub struct RoutingTable {
    pub own_id: NodeId,
    pub buckets: Vec<Bucket>,
    pub router_nodes: HashSet<SocketAddr>,
}

impl RoutingTable {
    pub fn new(own_id: NodeId, router_nodes: Vec<SocketAddr>) -> Self {
        Self {
            own_id,
            buckets: vec![Bucket::new()],
            router_nodes: router_nodes.into_iter().map(to_ipv6).collect(),
        }
    }

    pub fn next_refresh(&mut self) -> Option<ClientRequest> {
        let bucket_no = self.buckets.iter().position(|b| b.need_refresh())?;

        let bucket = &mut self.buckets[bucket_no];
        bucket.last_updated = Instant::now();
        log::trace!("Refresh bucket: {}", bucket_no);

        let c = bucket
            .live
            .iter()
            .chain(bucket.extra.iter())
            .max_by_key(|c| c.fail_count())?;

        if bucket.is_full() {
            Some(ClientRequest::Ping {
                id: c.id,
                addr: c.addr,
            })
        } else {
            let id = NodeId::gen_lz(bucket_no);
            Some(ClientRequest::BootStrap { target: id })
        }
    }

    pub fn add_router_node(&mut self, router: SocketAddr) {
        self.router_nodes.insert(router);
    }

    pub fn add_contact(&mut self, contact: &ContactRef<'_>) -> bool {
        let mut result = self.add_contact_impl(contact);
        loop {
            match result {
                BucketResult::Success => return true,
                BucketResult::Fail => return false,
                BucketResult::RequireSplit => {}
            }

            log::trace!("Split the buckets, before count: {}", self.buckets.len());
            self.split_bucket();
            log::trace!("Split the buckets, after count : {}", self.buckets.len());

            if let Some(last) = self.buckets.iter().last() {
                if last.live.len() > Bucket::MAX_LEN {
                    continue;
                }
            }

            result = self.add_contact_impl(contact);

            if let Some(last) = self.buckets.iter().last() {
                if last.live.is_empty() {
                    self.buckets.pop();
                    debug_assert_ne!(result, BucketResult::RequireSplit);
                }
            }
        }
    }

    pub fn find_closest<'a>(
        &'a self,
        target: &NodeId,
        out: &mut Vec<ContactRef<'a>>,
        count: usize,
    ) {
        let bucket_no = self.find_bucket(target);
        self.buckets[bucket_no].get_contacts(out);

        let len = self.buckets.len();
        let mut i = 1;

        while out.len() < count && (i <= bucket_no || bucket_no + i < len) {
            if i <= bucket_no {
                self.buckets[bucket_no - i].get_contacts(out);
            }
            if bucket_no + i < len {
                self.buckets[bucket_no + i].get_contacts(out);
            }
            i += 1;
        }

        out.sort_unstable_by_key(|c| target ^ c.id);
        out.truncate(count);
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.live.len()).sum()
    }

    pub fn len_extra(&self) -> usize {
        self.buckets.iter().map(|b| b.extra.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|b| b.live.is_empty())
    }

    pub fn find_contact(&mut self, id: &NodeId) -> Option<&mut Contact> {
        let idx = self.find_bucket(id);
        self.buckets[idx].live.iter_mut().find(|c| c.id == *id)
    }

    pub fn failed(&mut self, id: &NodeId) {
        if let Some(c) = self.find_contact(id) {
            c.timed_out();
        }
    }

    pub fn heard_from(&mut self, id: &NodeId) {
        let idx = self.find_bucket(id);
        let bucket = &mut self.buckets[idx];

        if let Some(c) = bucket.live.iter_mut().find(|c| c.id == *id) {
            c.status = ContactStatus::ALIVE | ContactStatus::QUERIED;
            c.clear_timeout();
            bucket.last_updated = Instant::now();
        }
    }

    fn add_contact_impl(&mut self, contact: &ContactRef<'_>) -> BucketResult {
        if self.router_nodes.contains(&contact.addr) {
            return BucketResult::Fail;
        }

        // Don't add ourselves
        if self.own_id == *contact.id {
            return BucketResult::Fail;
        }

        let bucket_index = self.find_bucket(contact.id);
        let bucket_count = self.buckets.len();
        let can_split = { bucket_index + 1 == bucket_count && bucket_count < 159 };

        let bucket = &mut self.buckets[bucket_index];

        if let Some(c) = bucket.live.iter_mut().find(|c| c.id == *contact.id) {
            if c.addr != contact.addr {
                return BucketResult::Fail;
            }

            c.timeout_count = Some(0);
            bucket.last_updated = Instant::now();
            return BucketResult::Success;
        }

        let maybe_extra = bucket
            .extra
            .iter_mut()
            .enumerate()
            .find(|(_, c)| c.id == *contact.id);

        let mut contact = contact.as_owned();

        if let Some((i, c)) = maybe_extra {
            if c.addr != contact.addr {
                return BucketResult::Fail;
            }

            c.timeout_count = Some(0);
            contact = bucket.extra.remove(i);
        }

        if bucket.live.len() < Bucket::MAX_LEN {
            if bucket.live.is_empty() {
                bucket.live.reserve(Bucket::MAX_LEN);
            }
            bucket.live.push(contact);
            bucket.last_updated = Instant::now();
            return BucketResult::Success;
        }

        if can_split {
            return BucketResult::RequireSplit;
        }

        if contact.is_confirmed() {
            let result = bucket.replace_node(&contact);
            if result != BucketResult::RequireSplit {
                return result;
            }
        }

        if bucket.extra.len() >= Bucket::MAX_LEN {
            if let Some(i) = bucket.extra.iter().position(|c| !c.is_pinged()) {
                bucket.extra.remove(i);
            } else {
                let result = bucket.replace_node(&contact);
                return if let BucketResult::Success = result {
                    BucketResult::Success
                } else {
                    BucketResult::Fail
                };
            }
        }

        if bucket.extra.is_empty() {
            bucket.extra.reserve(Bucket::MAX_LEN);
        }
        bucket.extra.push(contact);
        bucket.last_updated = Instant::now();
        BucketResult::Success
    }

    fn split_bucket(&mut self) {
        if self.buckets.is_empty() {
            return;
        }

        let index = self.buckets.len() - 1;
        let last_bucket = &mut self.buckets[index];

        let new_bucket = last_bucket.split(&self.own_id, index);
        self.buckets.push(new_bucket);
    }

    fn find_bucket(&self, id: &NodeId) -> usize {
        let idx = self.own_id.xlz(id);
        let last_idx = self.buckets.len().checked_sub(1).unwrap();
        idx.min(last_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut rt = RoutingTable::new(NodeId::all(0), vec![]);
        assert_eq!(rt.len(), 0);
        assert_eq!(rt.len_extra(), 0);
        assert_eq!(rt.buckets.len(), 1);

        let addr = SocketAddr::from(([0u8; 4], 100));

        // Add one contact
        assert!(rt.add_contact(&ContactRef {
            addr,
            id: &NodeId::all(1),
        }));
        assert_eq!(rt.len(), 1);
        assert_eq!(rt.len_extra(), 0);
        assert_eq!(rt.buckets.len(), 1);

        // Add the same contact again - Should add but size shouldn't change
        assert!(rt.add_contact(&ContactRef {
            addr,
            id: &NodeId::all(1),
        }));
        assert_eq!(rt.len(), 1);
        assert_eq!(rt.len_extra(), 0);
        assert_eq!(rt.buckets.len(), 1);

        // Add 7 more contacts, one bucket should be full now
        for i in 2..9 {
            assert!(rt.add_contact(&ContactRef {
                addr,
                id: &NodeId::all(i),
            }));
            assert_eq!(rt.len(), i as usize);
            assert_eq!(rt.len_extra(), 0);
            assert_eq!(rt.buckets.len(), 1);
        }

        // Add 1 more contacts - splits the bucket
        assert!(rt.add_contact(&ContactRef {
            addr,
            id: &NodeId::all(9),
        }));
        assert_eq!(rt.len(), 9);
        assert_eq!(rt.len_extra(), 0);
        assert_eq!(rt.buckets.len(), 6);
        assert_eq!(rt.buckets[4].live.len(), 2);
        assert_eq!(rt.buckets[5].live.len(), 7);

        // Add 6 more contacts - fill up bucket at index 4
        for i in 0..6 {
            let mut n = NodeId::all(9);
            n.0[19] = i as u8;
            assert!(rt.add_contact(&ContactRef { addr, id: &n }));
            assert_eq!(rt.len(), 10 + i);
            assert_eq!(rt.len_extra(), 0);
            assert_eq!(rt.buckets.len(), 6);
        }
        assert_eq!(rt.buckets[4].live.len(), 8);
        assert_eq!(rt.buckets[5].live.len(), 7);

        // Add 1 more contacts - goes into bucket index 4 extras
        let mut n = NodeId::all(9);
        n.0[19] = 6;
        assert!(rt.add_contact(&ContactRef { addr, id: &n }));
        assert_eq!(rt.len(), 15);
        assert_eq!(rt.len_extra(), 1);
        assert_eq!(rt.buckets.len(), 6);
        assert_eq!(rt.buckets[4].live.len(), 8);
        assert_eq!(rt.buckets[4].extra.len(), 1);
        assert_eq!(rt.buckets[5].live.len(), 7);
    }

    #[test]
    fn test_closest() {
        let mut table = RoutingTable::new(NodeId::all(0), vec![]);
        let addr = SocketAddr::from(([0u8; 4], 100));

        fn node(idx: usize) -> NodeId {
            let mut buf = [0; 20];
            buf[idx] = 1;
            NodeId::from(buf)
        }

        for i in 0..20 {
            let added = table.add_contact(&ContactRef { id: &node(i), addr });
            assert!(added, "Adding contact failed at {}", i);
        }

        let mut closest = Vec::with_capacity(20);
        table.find_closest(&NodeId::all(1), &mut closest, 20);

        let mut closest_iter = closest.into_iter();
        for i in 0..20 {
            assert_eq!(closest_iter.next().unwrap().id, &node(i));
        }

        assert!(closest_iter.next().is_none());
    }
}
