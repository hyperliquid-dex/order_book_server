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
            Err("List is empty".into())
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
