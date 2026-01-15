use std::collections::HashMap;

use crate::config::Committee;
use crate::core::SeqNumber;
use crate::messages::RandomCoin;
use crypto::PublicKey;

#[cfg(test)]
#[path = "tests/leader_tests.rs"]
pub mod leader_tests;

pub type LeaderElector = RandomLeaderElector;

pub struct RandomLeaderElector {
    names: Vec<PublicKey>,
    name_to_idx: HashMap<PublicKey, usize>,
    leader_window: usize,
    random_coins: HashMap<(SeqNumber, SeqNumber), RandomCoin>,
}

impl RandomLeaderElector {
    pub fn new(committee: &Committee, leader_window: usize) -> Self {
        let mut names: Vec<_> = committee.authorities.keys().cloned().collect();
        names.sort();

        let name_to_idx: HashMap<_, _> = names
            .iter()
            .enumerate()
            .map(|(idx, name)| (*name, idx))
            .collect();

        Self {
            names,
            name_to_idx,
            leader_window,
            random_coins: HashMap::new(),
        }
    }

    pub fn get_leader_idx(&self, height: SeqNumber) -> usize {
        height as usize % self.names.len()
    }

    pub fn get_leaders(&self, height: SeqNumber) -> Vec<PublicKey> {
        let mut leaders = Vec::new();

        let start = self.get_leader_idx(height);
        let end = start + self.leader_window;
        for i in start..end {
            leaders.push(self.names[(i+self.names.len())%self.names.len()]);
        }
        leaders
    }

    pub fn index_as_leader(&self, name: PublicKey, height: SeqNumber) -> Option<usize> {
        let start = self.get_leader_idx(height);
        let position = self.name_to_idx[&name];

        let idx = (position + self.names.len() - start) % self.names.len();
        (idx < self.leader_window).then(|| idx)
    }

    pub fn add_random_coin(&mut self, random_coin: RandomCoin) {
        self.random_coins
            .insert((random_coin.height, random_coin.round), random_coin);
    }

    pub fn get_coin_leader(&self, height: SeqNumber, round: SeqNumber) -> Option<PublicKey> {
        if !self.random_coins.contains_key(&(height, round)) {
            return None;
        }
        Some(self.random_coins.get(&(height, round)).unwrap().leader)
    }
}
