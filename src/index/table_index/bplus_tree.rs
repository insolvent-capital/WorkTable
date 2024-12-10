use std::fmt::Debug;
use std::mem::transmute;
use std::ops::RangeBounds;

use bplustree::iter::RawSharedIter;
use bplustree::BPlusTree;
use data_bucket::Link;

use crate::TableIndex;

pub struct BPlusTreeIter<'a, K, V>(pub RawSharedIter<'a, K, V, 128, 256>);

impl<'a, K, V> Iterator for BPlusTreeIter<'a, K, V>
where
    K: 'a + Clone + Ord,
    V: 'a,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe { transmute(self.0.next()) }
    }
}

// impl<K> TableIndex<K> for BPlusTree<K, Link>
// where
//     K: Debug + Clone + Ord + Send + Sync + 'static,
// {
//     fn insert(&self, key: K, link: Link) -> Result<(), (K, Link)> {
//         if let Some(link) = BPlusTree::insert(self, key.clone(), link) {
//             Err((key, link))
//         } else {
//             Ok(())
//         }
//     }
//
//     fn peek(&self, key: &K) -> Option<Link> {
//         BPlusTree::lookup(self, key, |link| *link)
//     }
//
//     fn remove(&self, key: &K) -> bool {
//         BPlusTree::remove(self, key).is_some()
//     }
//
//     fn iter<'a>(&'a self) -> impl Iterator<Item=(&'a K, &'a Link)>
//     where
//         K: 'a
//     {
//         BPlusTreeIter(BPlusTree::raw_iter(self))
//     }
//
//     fn range<'a, R: RangeBounds<K>>(&'a self, range: R) -> impl Iterator<Item=(&'a K, &'a Link)>
//     where
//         K: 'a
//     {
//         unimplemented!()
//     }
// }
