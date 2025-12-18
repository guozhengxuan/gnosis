use crate::config::Committee;
use crate::leader::RandomLeaderElector;
use crypto::{generate_keypair, PublicKey};
use rand::rngs::StdRng;
use rand::SeedableRng as _;

/// Create a test committee with a fixed seed for deterministic tests
fn test_committee(num_nodes: usize) -> (Committee, Vec<PublicKey>) {
    let mut rng = StdRng::from_seed([0; 32]);
    let mut keys = Vec::new();
    let mut authorities = Vec::new();

    for i in 0..num_nodes {
        let (name, _) = generate_keypair(&mut rng);
        keys.push(name);
        let address = format!("0.0.0.0:{}", i).parse().unwrap();
        let smvba_address = format!("0.0.0.0:{}", 100 + i).parse().unwrap();
        authorities.push((name, 0, 1, address, smvba_address));
    }

    let committee = Committee::new(authorities, 1);

    // Get sorted names (same order as in RandomLeaderElector)
    let mut sorted_names: Vec<_> = committee.authorities.keys().cloned().collect();
    sorted_names.sort();

    (committee, sorted_names)
}

#[test]
fn test_get_leader_idx_basic() {
    let (committee, _names) = test_committee(4);
    let elector = RandomLeaderElector::new(&committee, 2);

    // Test that leader index cycles through all nodes
    assert_eq!(elector.get_leader_idx(0), 0);
    assert_eq!(elector.get_leader_idx(1), 1);
    assert_eq!(elector.get_leader_idx(2), 2);
    assert_eq!(elector.get_leader_idx(3), 3);

    // Test wrapping around
    assert_eq!(elector.get_leader_idx(4), 0);
    assert_eq!(elector.get_leader_idx(5), 1);
    assert_eq!(elector.get_leader_idx(6), 2);
    assert_eq!(elector.get_leader_idx(7), 3);
    assert_eq!(elector.get_leader_idx(8), 0);
}

#[test]
fn test_get_leader_idx_large_height() {
    let (committee, _) = test_committee(4);
    let elector = RandomLeaderElector::new(&committee, 2);

    // Test with large heights
    assert_eq!(elector.get_leader_idx(100), 0); // 100 % 4 = 0
    assert_eq!(elector.get_leader_idx(101), 1); // 101 % 4 = 1
    assert_eq!(elector.get_leader_idx(999), 3); // 999 % 4 = 3
}

#[test]
fn test_get_leaders_window_2() {
    let (committee, names) = test_committee(4);
    let window = 2;
    let elector = RandomLeaderElector::new(&committee, window);

    // Height 0: primary leader at index 0, window covers indices 0, 1
    let leaders = elector.get_leaders(0);
    assert_eq!(leaders.len(), window);
    assert_eq!(leaders[0], names[0]);
    assert_eq!(leaders[1], names[1]);

    // Height 1: primary leader at index 1, window covers indices 1, 2
    let leaders = elector.get_leaders(1);
    assert_eq!(leaders.len(), window);
    assert_eq!(leaders[0], names[1]);
    assert_eq!(leaders[1], names[2]);

    // Height 2: primary leader at index 2, window covers indices 2, 3
    let leaders = elector.get_leaders(2);
    assert_eq!(leaders.len(), window);
    assert_eq!(leaders[0], names[2]);
    assert_eq!(leaders[1], names[3]);

    // Height 3: primary leader at index 3, window wraps around to cover indices 3, 0
    let leaders = elector.get_leaders(3);
    assert_eq!(leaders.len(), window);
    assert_eq!(leaders[0], names[3]);
    assert_eq!(leaders[1], names[0]); // Wrapped around
}

#[test]
fn test_get_leaders_window_3() {
    let (committee, names) = test_committee(5);
    let window = 3;
    let elector = RandomLeaderElector::new(&committee, window);

    // Height 0: covers indices 0, 1, 2
    let leaders = elector.get_leaders(0);
    assert_eq!(leaders.len(), window);
    assert_eq!(leaders[0], names[0]);
    assert_eq!(leaders[1], names[1]);
    assert_eq!(leaders[2], names[2]);

    // Height 3: covers indices 3, 4, 0 (wraps around)
    let leaders = elector.get_leaders(3);
    assert_eq!(leaders.len(), window);
    assert_eq!(leaders[0], names[3]);
    assert_eq!(leaders[1], names[4]);
    assert_eq!(leaders[2], names[0]); // Wrapped around

    // Height 4: covers indices 4, 0, 1 (wraps around)
    let leaders = elector.get_leaders(4);
    assert_eq!(leaders.len(), window);
    assert_eq!(leaders[0], names[4]);
    assert_eq!(leaders[1], names[0]); // Wrapped around
    assert_eq!(leaders[2], names[1]); // Wrapped around
}

#[test]
fn test_get_leaders_full_window() {
    let (committee, names) = test_committee(4);
    let window = 4; // Window equals number of nodes
    let elector = RandomLeaderElector::new(&committee, window);

    // All nodes are leaders regardless of height
    let leaders = elector.get_leaders(0);
    assert_eq!(leaders.len(), window);
    assert_eq!(leaders, names);

    let leaders = elector.get_leaders(2);
    assert_eq!(leaders.len(), window);
    // Should be [names[2], names[3], names[0], names[1]]
    assert_eq!(leaders[0], names[2]);
    assert_eq!(leaders[1], names[3]);
    assert_eq!(leaders[2], names[0]);
    assert_eq!(leaders[3], names[1]);
}

#[test]
fn test_get_leaders_window_1() {
    let (committee, names) = test_committee(4);
    let window = 1; // Only primary leader
    let elector = RandomLeaderElector::new(&committee, window);

    // Height 0: only index 0
    let leaders = elector.get_leaders(0);
    assert_eq!(leaders.len(), 1);
    assert_eq!(leaders[0], names[0]);

    // Height 2: only index 2
    let leaders = elector.get_leaders(2);
    assert_eq!(leaders.len(), 1);
    assert_eq!(leaders[0], names[2]);
}

#[test]
fn test_index_as_leader_within_window() {
    let (committee, names) = test_committee(4);
    let window = 2;
    let elector = RandomLeaderElector::new(&committee, window);

    // Height 0: primary leader at index 0, window covers [0, 1]
    // Node at index 0 is the primary leader (offset 0)
    assert_eq!(elector.index_as_leader(names[0], 0), Some(0));
    // Node at index 1 is at offset 1
    assert_eq!(elector.index_as_leader(names[1], 0), Some(1));
    // Node at index 2 is outside the window
    assert_eq!(elector.index_as_leader(names[2], 0), None);
    // Node at index 3 is outside the window
    assert_eq!(elector.index_as_leader(names[3], 0), None);

    // Height 1: primary leader at index 1, window covers [1, 2]
    assert_eq!(elector.index_as_leader(names[1], 1), Some(0)); // Primary
    assert_eq!(elector.index_as_leader(names[2], 1), Some(1)); // Offset 1
    assert_eq!(elector.index_as_leader(names[3], 1), None);    // Outside
    assert_eq!(elector.index_as_leader(names[0], 1), None);    // Outside
}

#[test]
fn test_index_as_leader_wrap_around() {
    let (committee, names) = test_committee(4);
    let window = 2;
    let elector = RandomLeaderElector::new(&committee, window);

    // Height 3: primary leader at index 3, window covers [3, 0] (wraps around)
    assert_eq!(elector.index_as_leader(names[3], 3), Some(0)); // Primary at index 3
    assert_eq!(elector.index_as_leader(names[0], 3), Some(1)); // Wrapped to index 0, offset 1
    assert_eq!(elector.index_as_leader(names[1], 3), None);    // Outside window
    assert_eq!(elector.index_as_leader(names[2], 3), None);    // Outside window
}

#[test]
fn test_index_as_leader_window_3() {
    let (committee, names) = test_committee(5);
    let window = 3;
    let elector = RandomLeaderElector::new(&committee, window);

    // Height 4: primary leader at index 4, window covers [4, 0, 1]
    assert_eq!(elector.index_as_leader(names[4], 4), Some(0)); // Primary
    assert_eq!(elector.index_as_leader(names[0], 4), Some(1)); // Offset 1 (wrapped)
    assert_eq!(elector.index_as_leader(names[1], 4), Some(2)); // Offset 2 (wrapped)
    assert_eq!(elector.index_as_leader(names[2], 4), None);    // Outside window
    assert_eq!(elector.index_as_leader(names[3], 4), None);    // Outside window
}

#[test]
fn test_index_as_leader_all_positions() {
    let (committee, names) = test_committee(6);
    let window = 3;
    let elector = RandomLeaderElector::new(&committee, window);

    // Height 2: primary at index 2, window covers [2, 3, 4]
    // Test all nodes
    assert_eq!(elector.index_as_leader(names[0], 2), None);    // Position 0, distance from 2 is 4 (wraparound), outside
    assert_eq!(elector.index_as_leader(names[1], 2), None);    // Position 1, distance from 2 is 5 (wraparound), outside
    assert_eq!(elector.index_as_leader(names[2], 2), Some(0)); // Primary
    assert_eq!(elector.index_as_leader(names[3], 2), Some(1)); // Offset 1
    assert_eq!(elector.index_as_leader(names[4], 2), Some(2)); // Offset 2
    assert_eq!(elector.index_as_leader(names[5], 2), None);    // Outside window, offset would be 3
}

#[test]
fn test_index_as_leader_boundary() {
    let (committee, names) = test_committee(5);
    let window = 2;
    let elector = RandomLeaderElector::new(&committee, window);

    // Height 0: window covers [0, 1]
    // Test boundary: offset 0 and 1 should be in, 2 and beyond should be out
    assert_eq!(elector.index_as_leader(names[0], 0), Some(0));
    assert_eq!(elector.index_as_leader(names[1], 0), Some(1));
    assert_eq!(elector.index_as_leader(names[2], 0), None); // offset 2, should be None
}

#[test]
fn test_consistency_between_get_leaders_and_index_as_leader() {
    let (committee, names) = test_committee(5);
    let window = 3;
    let elector = RandomLeaderElector::new(&committee, window);

    for height in 0..10 {
        let leaders = elector.get_leaders(height);

        // All nodes returned by get_leaders should have a valid index_as_leader
        for (expected_offset, &leader) in leaders.iter().enumerate() {
            let actual_offset = elector.index_as_leader(leader, height);
            assert_eq!(
                actual_offset,
                Some(expected_offset),
                "Height {}: leader {:?} should have offset {}, got {:?}",
                height,
                leader,
                expected_offset,
                actual_offset
            );
        }

        // All nodes NOT in get_leaders should return None from index_as_leader
        for &name in &names {
            if !leaders.contains(&name) {
                assert_eq!(
                    elector.index_as_leader(name, height),
                    None,
                    "Height {}: node {:?} not in leaders list should return None",
                    height,
                    name
                );
            }
        }
    }
}

#[test]
fn test_circular_property() {
    let (committee, names) = test_committee(4);
    let window = 2;
    let elector = RandomLeaderElector::new(&committee, window);

    // Verify that leaders shift circularly as height increases
    for base_height in 0..4 {
        let leaders_at_base = elector.get_leaders(base_height);
        let leaders_at_plus_4 = elector.get_leaders(base_height + 4);

        // Leaders should be the same after one full cycle
        assert_eq!(
            leaders_at_base, leaders_at_plus_4,
            "Leaders should repeat after {} heights",
            names.len()
        );
    }
}
