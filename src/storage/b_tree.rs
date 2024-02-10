use std::{cmp::Ordering, io::Bytes, ops::Range, u16, u64};

const HEADER: usize = 4;

const BTREE_PAGE_SIZE: usize = 4096;
const BTREE_MAX_KEY_SIZE: usize = 1000;
const BTREE_MAX_VAL_SIZE: usize = 3000;

#[derive(Debug, Clone)]
pub struct BNode {
    data: Vec<u8>,
}

impl BNode {
    // btyoe and nkeys
    // | type | nkeys |  pointers  |   offsets  | key-values
    // |  2B  |   2B  | nkeys * 8B | nkeys * 2B | ...
    pub fn btype(&self) -> u16 {
        u16::from_le_bytes(self.data[..2].try_into().unwrap())
    }

    pub fn nkeys(&self) -> u16 {
        u16::from_le_bytes(self.data[2..4].try_into().unwrap())
    }

    // set header
    pub fn set_header(&mut self, btype: u16, keys: u16) {
        self.data[0..2].copy_from_slice(&btype.to_le_bytes());
        self.data[2..4].copy_from_slice(&keys.to_le_bytes());
    }

    // points
    pub fn get_ptr(&self, idx: u16) -> u64 {
        assert!(idx < self.nkeys());

        let pos = Self::ptr_pose(idx);
        u64::from_le_bytes(self.data[pos..].try_into().unwrap())
    }

    pub fn set_ptr(&mut self, idx: u16, val: u64) {
        assert!(idx < self.nkeys());

        let pos = Self::ptr_pose(idx);
        self.data[pos..pos + 8].copy_from_slice(&val.to_le_bytes());
    }

    fn ptr_pose(idx: u16) -> usize {
        HEADER + 8 * idx as usize
    }

    // offset list
    fn offset_pose(&self, idx: u16) -> usize {
        assert!(1 <= idx && idx <= self.nkeys());

        HEADER + 8 * self.nkeys() as usize + 2 * (idx as usize - 1)
    }

    pub fn get_offset(&self, idx: u16) -> u16 {
        if idx == 0 {
            return 0;
        }

        let pos = self.offset_pose(idx);
        u16::from_le_bytes(self.data[pos..].try_into().unwrap())
    }

    pub fn set_offset(&mut self, idx: u16, offset: u16) {
        let pos = self.offset_pose(idx);
        self.data[pos..pos + 2].copy_from_slice(&offset.to_le_bytes());
    }

    // key-values
    // | klen | vlen | key | val |
    // |  2B  |  2B  | ... | ... |
    pub fn kv_pos(&self, idx: u16) -> usize {
        assert!(idx <= self.nkeys());

        HEADER
            + 8 * self.nkeys() as usize
            + 2 * self.nkeys() as usize
            + self.get_offset(idx) as usize
    }

    pub fn get_key(&self, idx: u16) -> Vec<u8> {
        assert!(idx < self.nkeys());

        let pos = self.kv_pos(idx);
        let key_len = u16::from_le_bytes(self.data[pos..].try_into().unwrap());

        self.data[pos + 4..pos + 4 + key_len as usize].to_vec()
    }

    pub fn get_val(&self, idx: u16) -> Vec<u8> {
        assert!(idx < self.nkeys());

        let pos = self.kv_pos(idx);
        let key_len = u16::from_le_bytes(self.data[pos..].try_into().unwrap());
        let val_len = u16::from_le_bytes(self.data[pos + 2..].try_into().unwrap());

        let base = pos + 4 + key_len as usize;
        self.data[base..base + val_len as usize].to_vec()
    }

    pub fn n_bytes(&self) -> u16 {
        self.kv_pos(self.nkeys()) as u16
    }

    // 在节点中查找key
    pub fn node_lookup_le(&self, key: &Vec<u8>) -> u16 {
        let nkeys = self.nkeys();
        let mut found = 0_u16;

        for i in 1..nkeys {
            let cmp = self.get_key(i).cmp(key);
            if cmp != Ordering::Greater {
                found = i;
            } else {
                break;
            }
        }

        found
    }

    // 将key value 复制到当前节点
    pub fn node_append_range(&mut self, old: &BNode, dst_new: u16, src_old: u16, n: u16) {
        assert!(src_old + n < old.nkeys());
        assert!(dst_new + n < self.nkeys());

        if n == 0 {
            return;
        }

        // copy pointer
        for i in 0..n {
            self.set_ptr(i, old.get_ptr(i));
        }

        // copy offset
        let dst_begin = self.get_offset(dst_new);
        let src_begin = old.get_offset(src_old);
        for i in 1..n {
            let offset = dst_begin + old.get_offset(src_old + i) - src_begin;
            self.set_offset(dst_new + i, offset);
        }

        // copy k-v
        let begin = old.kv_pos(src_old);
        let end = old.kv_pos(src_old + n);
        self.data.copy_from_slice(&old.data[begin..end]);
    }

    // 插入k-v
    pub fn node_append_kv(&mut self, idx: u16, ptr: u64, key: Vec<u8>, val: Vec<u8>) {
        // 插入子节点的指针
        self.set_ptr(idx, ptr);

        // 处理k-v
        let pos = self.kv_pos(idx);
        self.data[pos..pos + 2].copy_from_slice(&u16::to_le_bytes(key.len() as u16));
        self.data[pos + 2..pos + 4].copy_from_slice(&u16::to_le_bytes(val.len() as u16));
        self.data[pos + 4..pos + 4 + key.len()].copy_from_slice(&key);
        self.data[pos + 4 + key.len()..pos + 4 + key.len() + val.len()].copy_from_slice(&val);

        self.set_offset(
            idx + 1,
            self.get_offset(idx) + 4 + key.len() as u16 + val.len() as u16,
        );
    }

    pub fn leaf_insert(&mut self, old: &BNode, idx: u16, key: Vec<u8>, val: Vec<u8>) {
        self.set_header(NodeType::Leaf as u16, old.nkeys() + 1);
        self.node_append_range(old, 0, 0, idx);
        self.node_append_kv(idx, 0, key, val);
        self.node_append_range(old, idx + 1, idx, old.nkeys() - idx);
    }

    pub fn leaf_update(&mut self, old: &BNode, idx: u16, key: Vec<u8>, val: Vec<u8>) {
        self.set_header(NodeType::Leaf as u16, old.nkeys());
        if idx > 0 {
            self.node_append_range(old, 0, 0, idx - 1);
        }
        self.node_append_kv(idx, 0, key, val);
        self.node_append_range(old, idx + 1, idx + 1, old.nkeys() - idx);
    }

    // 分割节点
    pub fn node_split_3(&mut self) -> (u16, Vec<BNode>) {
        if self.n_bytes() as usize <= BTREE_PAGE_SIZE {
            // self.data = self.data[..BTREE_PAGE_SIZE];
            return (1, vec![self.clone()]);
        }

        let mut left = BNode {
            data: vec![0; 2 * BTREE_PAGE_SIZE],
        };
        let mut right = BNode {
            data: vec![0; BTREE_PAGE_SIZE],
        };

        self.node_split_2(&mut left, &mut right);
        if left.n_bytes() as usize <= BTREE_PAGE_SIZE {
            left.data = left.data[..BTREE_PAGE_SIZE].to_vec();
            return (2, vec![left, right]);
        }

        let mut left_left = BNode {
            data: vec![0; BTREE_PAGE_SIZE],
        };
        let mut middle = BNode {
            data: vec![0; BTREE_PAGE_SIZE],
        };
        self.node_split_2(&mut left_left, &mut middle);
        assert!(left_left.n_bytes() as usize <= BTREE_PAGE_SIZE);

        return (3, vec![left_left, middle, right]);
    }

    pub fn node_split_2(&self, left: &mut BNode, right: &mut BNode) {
        todo!()
    }
}

#[derive(Debug)]
#[repr(u16)]
pub enum NodeType {
    Node = 1,
    Leaf = 2,
}

impl From<u16> for NodeType {
    fn from(value: u16) -> Self {
        match value {
            1 => NodeType::Node,
            2 => NodeType::Leaf,
            _ => panic!("Invalid value"),
        }
    }
}

#[derive(Debug)]
pub struct BTree {
    root: u64,
}

impl BTree {
    pub fn new(&self, node: &BNode) -> u64 {
        todo!()
    }

    pub fn get(&self, ptr: u64) -> BNode {
        todo!()
    }

    pub fn del(&self, root: u64) {
        todo!()
    }

    // 向node中插入k-v，有可能会导致节点分裂
    pub fn tree_insert(&self, node: &BNode, key: Vec<u8>, val: Vec<u8>) -> BNode {
        let mut new_node = BNode {
            data: vec![0; 2 * BTREE_PAGE_SIZE],
        };

        let idx = node.node_lookup_le(&key);
        match NodeType::try_from(node.btype()) {
            Ok(node_type) => match node_type {
                NodeType::Leaf => {
                    if key.eq(&node.get_key(idx)) {
                        new_node.leaf_update(node, idx, key, val);
                    } else {
                        new_node.leaf_insert(node, idx + 1, key, val);
                    }
                }
                NodeType::Node => {
                    self.node_insert(&new_node, node, idx, key, val);
                }
            },
            Err(_) => panic!("node error"),
        };

        new_node
    }

    // 更新内部节点
    pub fn node_replace_kid_n(
        &self,
        new_node: &mut BNode,
        old: &BNode,
        idx: u16,
        kids: Vec<BNode>,
    ) {
        let inc = kids.len() as u16;
        new_node.set_header(NodeType::Node as u16, old.nkeys() + inc - 1);
        new_node.node_append_range(old, 0, 0, idx);
        for (i, node) in kids.iter().enumerate() {
            new_node.node_append_kv(idx + i as u16, self.new(node), node.get_key(0), vec![]);
        }

        new_node.node_append_range(old, idx + inc, idx + 1, old.nkeys() - (idx + 1));
    }

    // 处理node节点
    pub fn node_insert(
        &self,
        new_node: &BNode,
        node: &BNode,
        idx: u16,
        key: Vec<u8>,
        val: Vec<u8>,
    ) {
        let kid_ptr = node.get_ptr(idx);
        let kid_node = self.get(kid_ptr);

        self.del(kid_ptr);
        let kid_node = self.tree_insert(&kid_node, key, val);
    }
}

fn init() {
    let node1max = HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE;
    assert!(node1max <= BTREE_PAGE_SIZE)
}
