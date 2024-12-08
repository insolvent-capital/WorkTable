use data_bucket::Link;
use scc::ebr::Guard;
use std::mem::transmute;
use std::ops::RangeBounds;
use std::rc::Rc;

pub trait TableIndex<K> {
    fn insert(&self, key: K, link: Link) -> Result<(), (K, Link)>;
    fn peek(&self, key: &K) -> Option<Link>;
    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a Link)>
    where
        K: 'a;
    fn range<'a, Q, R: RangeBounds<K>>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (&'a K, &'a Link)>
    where
        K: 'a;
}

impl<K> TableIndex<K> for scc::TreeIndex<K, Link>
where
    K: Clone + Ord + Send + Sync + 'static,
{
    fn insert(&self, key: K, link: Link) -> Result<(), (K, Link)> {
        scc::TreeIndex::insert(self, key, link)
    }

    fn peek(&self, key: &K) -> Option<Link> {
        let guard = Guard::new();
        scc::TreeIndex::peek(self, key, &guard).cloned()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a Link)>
    where
        K: 'a,
    {
        let guard = Guard::new();
        let guard: &'a Guard = unsafe { transmute(&guard) };
        scc::TreeIndex::iter(self, guard)
    }

    fn range<'a, Q, R: RangeBounds<K>>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (&'a K, &'a Link)>
    where
        K: 'a,
    {
        let guard = Guard::new();
        let guard: &'a Guard = unsafe { transmute(&guard) };
        scc::TreeIndex::range(self, range, guard)
    }
}
