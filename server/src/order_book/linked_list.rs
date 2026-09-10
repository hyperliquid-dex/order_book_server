use crate::prelude::*;
use slab::Slab;
use std::{collections::HashMap, hash::Hash, marker::PhantomData};

#[derive(Clone)]
struct Node<K, T> {
    key: K,
    value: T,
    next: Option<usize>,
    prev: Option<usize>,
}

impl<K, T> Node<K, T> {
    pub(crate) const fn new(key: K, value: T) -> Self {
        Self { key, value, next: None, prev: None }
    }
}

#[derive(Clone)]
// Implicit assumption is that when we remove a node, it is never used again
pub(crate) struct LinkedList<K, T> {
    key_to_sid: HashMap<K, usize>,
    slab: Slab<Node<K, T>>,
    head: Option<usize>,
    tail: Option<usize>,
    phantom_data: PhantomData<T>,
}

impl<K: Clone + Eq + Hash, T: Clone> LinkedList<K, T> {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { key_to_sid: HashMap::new(), slab: Slab::new(), head: None, tail: None, phantom_data: PhantomData }
    }

    pub(crate) fn push_back(&mut self, key: K, value: T) -> bool {
        if self.key_to_sid.contains_key(&key) {
            false
        } else {
            let node = Node::new(key.clone(), value);
            let sid = self.slab.insert(node);
            self.key_to_sid.insert(key, sid);
            match self.tail {
                None => {
                    self.head = Some(sid);
                    self.tail = Some(sid);
                }
                Some(t) => {
                    let tail_order = &mut self.slab[t];
                    tail_order.next = Some(sid);
                    let new_order = &mut self.slab[sid];
                    new_order.prev = Some(t);
                    self.tail = Some(sid);
                }
            }
            true
        }
    }

    // Inserts `key` directly in front of `before`. Fails when `before` is not in the list or
    // `key` already is.
    pub(crate) fn insert_before(&mut self, before: &K, key: K, value: T) -> bool {
        if self.key_to_sid.contains_key(&key) {
            return false;
        }
        let Some(&before_sid) = self.key_to_sid.get(before) else {
            return false;
        };
        let mut node = Node::new(key.clone(), value);
        let prev = self.slab[before_sid].prev;
        node.prev = prev;
        node.next = Some(before_sid);
        let sid = self.slab.insert(node);
        self.key_to_sid.insert(key, sid);
        self.slab[before_sid].prev = Some(sid);
        match prev {
            Some(p) => self.slab[p].next = Some(sid),
            None => self.head = Some(sid),
        }
        true
    }

    #[must_use]
    pub(crate) const fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    pub(crate) fn head_value_ref_mut_unsafe(&mut self) -> Option<&mut T> {
        self.head.as_ref().map(|&h| &mut self.slab[h].value)
    }

    pub(crate) fn remove_front(&mut self) -> Result<()> {
        if let Some(h) = self.head {
            let new_head = {
                let head_order = self.slab.remove(h);
                self.key_to_sid.remove(&head_order.key);
                head_order.next
            };
            match new_head {
                None => {
                    self.head = None;
                    self.tail = None;
                }
                Some(n) => {
                    self.head = Some(n);
                    let new_order = &mut self.slab[n];
                    new_order.prev = None;
                }
            }
            Ok(())
        } else {
            Err(OrderBookError::Generic("List is empty".to_string()))
        }
    }

    pub(crate) fn remove_node(&mut self, key: K) -> bool {
        if let Some((_, sid)) = self.key_to_sid.remove_entry(&key) {
            let (prev, next) = {
                let order = self.slab.remove(sid);
                (order.prev, order.next)
            };
            if let Some(p) = prev {
                let prev_order = &mut self.slab[p];
                prev_order.next = next;
            } else {
                self.head = next;
            }
            if let Some(n) = next {
                let next_order = &mut self.slab[n];
                next_order.prev = prev;
            } else {
                self.tail = prev;
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn node_value_mut(&mut self, key: &K) -> Option<&mut T> {
        if let Some(sid) = self.key_to_sid.get(key) { Some(&mut self.slab[*sid].value) } else { None }
    }

    #[must_use]
    pub(crate) fn to_vec(&self) -> Vec<&T> {
        let mut res = Vec::new();
        let mut cur = self.head;
        while let Some(c) = cur {
            let node = &self.slab[c];
            res.push(&node.value);
            cur = node.next;
        }
        res
    }

    pub(crate) fn fold<F, Acc>(&self, mut init: Acc, f: F) -> Acc
    where
        F: Fn(&mut Acc, &T),
    {
        let mut cur = self.head;
        while let Some(c) = cur {
            let node = &self.slab[c];
            f(&mut init, &node.value);
            cur = node.next;
        }
        init
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use std::collections::VecDeque;

    #[must_use]
    fn to_rev_vec<K: Clone + Eq + Hash, T: Clone>(list: &LinkedList<K, T>) -> Vec<&T> {
        let mut res = Vec::new();
        let mut cur = list.tail;
        while let Some(c) = cur {
            let node = &list.slab[c];
            res.push(&node.value);
            cur = node.prev;
        }
        res
    }

    #[test]
    fn simple_linked_list_test() -> Result<()> {
        let mut deque = (0..11).collect::<VecDeque<_>>();
        let mut keys = Vec::new();
        let mut list = LinkedList::new();
        for &elt in &deque {
            keys.push(elt);
            list.push_back(elt, elt);
        }

        assert_vec_deque_list_eq(&deque, &list);

        list.remove_front()?;
        deque.pop_front();

        assert_vec_deque_list_eq(&deque, &list);

        list.remove_front()?;
        deque.pop_front();

        assert_vec_deque_list_eq(&deque, &list);

        list.remove_node(keys[4]);
        deque.remove(2);

        assert_vec_deque_list_eq(&deque, &list);

        for _ in 0..5 {
            list.remove_front()?;
            deque.pop_front();
            assert_vec_deque_list_eq(&deque, &list);
        }

        for k in keys.iter().skip(8) {
            list.remove_node(*k);
            deque.pop_front();
            assert_vec_deque_list_eq(&deque, &list);
        }

        assert!(list.is_empty());
        Ok(())
    }

    #[test]
    fn edge_case_single_element_test() -> Result<()> {
        let mut list = LinkedList::new();
        // Test push_back and remove_front on single element
        list.push_back(42, "value");
        assert!(!list.is_empty());
        assert!(list.head_value_ref_mut_unsafe().is_some());
        list.remove_front()?;
        assert!(list.is_empty());
        Ok(())
    }

    #[test]
    fn edge_case_remove_front_empty_test() -> Result<()> {
        let mut list = LinkedList::new();
        // Removing from empty list should return error
        let result = list.remove_front();
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn edge_case_remove_nonexistent_node() {
        let mut list = LinkedList::new();
        list.push_back(1, "val1");
        list.push_back(2, "val2");
        // Removing non-existent node should return false
        assert!(!list.remove_node(99));
        // Original list should be unchanged
        let mut count = 0;
        let mut current = list.head;
        while let Some(c) = current {
            count += 1;
            current = list.slab[c].next;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn stress_test_large_list() -> Result<()> {
        let mut list = LinkedList::new();
        let size = 10_000usize;
        // Insert many elements
        for i in 0..size {
            list.push_back(i, format!("value_{}", i));
        }
        // Verify all elements present
        assert!(!list.is_empty());
        assert_eq!(list.to_vec().len(), size);
        // Remove half
        for i in 0..(size / 2) {
            assert!(list.remove_node(i));
        }
        // Verify remaining elements
        assert_eq!(list.to_vec().len(), size / 2);
        Ok(())
    }

    #[test]
    fn insert_before_tail() -> Result<()> {
        let mut list = LinkedList::new();
        // Insert tail element first
        list.push_back(1, "tail1");
        // Insert before it
        assert!(list.insert_before(&1, 0, "head0"));
        // Verify order
        let values: Vec<&str> = list.to_vec().iter().map(|s| *s).collect();
        assert_eq!(values, vec!["head0", "tail1"]);
        Ok(())
    }

    #[test]
    fn insert_before_middle() -> Result<()> {
        let mut list = LinkedList::new();
        // Insert sequence: 1, 2, 3
        list.push_back(1, "first");
        list.push_back(2, "second");
        list.push_back(3, "third");
        // Insert 1.5 before 2
        assert!(list.insert_before(&2, 99, "middle"));
        // Verify order
        let values: Vec<&str> = list.to_vec().iter().map(|s| *s).collect();
        assert_eq!(values, vec!["first", "middle", "second", "third"]);
        Ok(())
    }

    #[test]
    fn duplicate_key_insertion() {
        let mut list = LinkedList::new();
        // Insert key 1
        assert!(list.push_back(1, "value1"));
        // Try to insert same key again - should fail
        assert!(!list.push_back(1, "value2"));
        // Original should remain
        let values: Vec<&str> = list.to_vec().iter().map(|s| *s).collect();
        assert_eq!(values, vec!["value1"]);
    }

    #[test]
    fn insert_before_after_removals() -> Result<()> {
        let mut list = LinkedList::new();
        list.push_back(1, "first");
        list.push_back(2, "second");
        list.push_back(3, "third");
        // Remove middle element
        assert!(list.remove_node(2));
        // Insert before what was position 3 (now position 2)
        assert!(list.insert_before(&3, 99, "new_middle"));
        // Verify order: first, new_middle, third
        let values: Vec<&str> = list.to_vec().iter().map(|s| *s).collect();
        assert_eq!(values, vec!["first", "new_middle", "third"]);
        Ok(())
    }

    #[test]
    fn insert_before_test() -> Result<()> {
        let mut list = LinkedList::new();
        // Missing anchor on an empty list
        assert!(!list.insert_before(&0, 1, 1));
        for elt in [10, 20, 30] {
            list.push_back(elt, elt);
        }
        // Insert at head
        assert!(list.insert_before(&10, 5, 5));
        // Insert in the middle
        assert!(list.insert_before(&30, 25, 25));
        // Missing anchor
        assert!(!list.insert_before(&99, 40, 40));
        // Duplicate key
        assert!(!list.insert_before(&10, 25, 25));
        let expected = VecDeque::from([5, 10, 20, 25, 30]);
        assert_vec_deque_list_eq(&expected, &list);

        // Removals still work around inserted nodes
        list.remove_front()?;
        assert!(list.remove_node(25));
        let expected = VecDeque::from([10, 20, 30]);
        assert_vec_deque_list_eq(&expected, &list);

        // Re-inserting a previously removed key in front of the tail
        assert!(list.insert_before(&30, 25, 26));
        let expected = VecDeque::from([10, 20, 26, 30]);
        assert_vec_deque_list_eq(&expected, &list);
        Ok(())
    }

    fn assert_vec_deque_list_eq<K: Clone + Eq + Hash, T: Debug + Clone + Eq + PartialEq>(
        deque: &VecDeque<T>,
        list: &LinkedList<K, T>,
    ) {
        let evec = deque.iter().cloned().collect_vec();
        for (a, b) in list.to_vec().iter().zip(evec.iter()) {
            assert_eq!(**a, *b);
        }
        let mut rev = evec;
        rev.reverse();
        for (a, b) in to_rev_vec(list).iter().zip(rev.iter()) {
            assert_eq!(**a, *b);
        }
    }
}
