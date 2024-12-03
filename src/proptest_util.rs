/// A convenient place to put strategies for generating common value types.
/// See tests below for example usage.
#[cfg(test)]
pub(crate) mod arbitrary {
    use crate::bytes_range;
    use crate::bytes_range::BytesRange;
    use crate::proptest_util::rng;
    use bytes::{BufMut, Bytes, BytesMut};
    use proptest::arbitrary::any;
    use proptest::collection::vec;
    use proptest::prelude::{Just, Strategy};
    use proptest::prop_oneof;
    use proptest::test_runner::TestRng;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    pub(crate) fn bytes(size: usize) -> impl Strategy<Value = Bytes> {
        vec(any::<u8>(), 0..size).prop_map(|v| Bytes::from(v))
    }

    pub(crate) fn nonempty_bytes(size: usize) -> impl Strategy<Value = Bytes> {
        vec(any::<u8>(), 1..size).prop_map(|v| Bytes::from(v))
    }

    /// Get a deterministic RNG which can be used to generate random samples
    /// in a property test case.  
    pub(crate) fn rng() -> impl Strategy<Value = TestRng> {
        vec(any::<u8>(), 32).prop_map(|seed| {
            let mut s: [u8; 32] = Default::default();
            s.clone_from_slice(&seed);
            rng::new_test_rng(Some(s))
        })
    }

    fn nonempty_bounded_range(size: usize) -> impl Strategy<Value = BytesRange> {
        (bytes(size), bytes(size))
            .prop_filter_map("Filter non-empty ranges", nonempty_range_filter)
            .prop_flat_map(|(start, end)| {
                prop_oneof![
                    Just(BytesRange::new(
                        Included(start.clone()),
                        Included(end.clone())
                    )),
                    Just(BytesRange::new(
                        Excluded(start.clone()),
                        Excluded(end.clone())
                    )),
                    Just(BytesRange::new(
                        Included(start.clone()),
                        Excluded(end.clone())
                    )),
                    Just(BytesRange::new(
                        Excluded(start.clone()),
                        Included(end.clone())
                    )),
                ]
            })
    }

    fn nonempty_partial_bounded_range(size: usize) -> impl Strategy<Value = BytesRange> {
        nonempty_bytes(size).prop_flat_map(|a| {
            prop_oneof![
                Just(BytesRange::new(Unbounded, Included(a.clone()))),
                Just(BytesRange::new(Unbounded, Excluded(a.clone()))),
                Just(BytesRange::new(Included(a.clone()), Unbounded)),
                Just(BytesRange::new(Excluded(a.clone()), Unbounded)),
            ]
        })
    }

    fn extend_bytes(a: &Bytes, value: u8) -> Bytes {
        let mut b = BytesMut::from(a.as_ref());
        b.put_u8(value);
        b.freeze()
    }

    fn arbitrary_single_point_bounded_range(size: usize) -> impl Strategy<Value = BytesRange> {
        bytes(size).prop_flat_map(|a| {
            let a_extended = extend_bytes(&a, u8::MIN);
            prop_oneof![
                Just(BytesRange::new(Included(a.clone()), Included(a.clone()))),
                Just(BytesRange::new(
                    Included(a.clone()),
                    Excluded(a_extended.clone())
                )),
                Just(BytesRange::new(
                    Excluded(a.clone()),
                    Included(a_extended.clone())
                )),
            ]
        })
    }

    /// Valid ranges which just have the single empty byte array.
    /// Handled as a special case because an exclusive upper bounded
    /// range with empty bytes (i.e. `..Bytes::new()`) is empty.
    fn nonempty_range_edge_cases() -> impl Strategy<Value = BytesRange> {
        let empty_bytes = Bytes::new();
        prop_oneof![
            Just(BytesRange::new(Unbounded, Included(empty_bytes.clone()))),
            Just(BytesRange::new(Included(empty_bytes.clone()), Unbounded)),
            Just(BytesRange::new(Excluded(empty_bytes.clone()), Unbounded)),
        ]
    }

    fn min_and_max(a: Bytes, b: Bytes) -> (Bytes, Bytes) {
        if a < b {
            (a, b)
        } else {
            (b, a)
        }
    }

    fn nonempty_range_filter(tuple: (Bytes, Bytes)) -> Option<(Bytes, Bytes)> {
        let (a, b) = tuple;
        if a == b {
            None
        } else {
            let (start, end) = min_and_max(a, b);
            if bytes_range::is_prefix_increment(&start, &end) {
                None
            } else {
                Some((start, end))
            }
        }
    }

    pub(crate) fn empty_range(size: usize) -> impl Strategy<Value = BytesRange> {
        prop_oneof![
            Just(BytesRange::new(Unbounded, Excluded(Bytes::new()))),
            bytes(size).prop_flat_map(|a| {
                prop_oneof![
                    Just(BytesRange::new(Excluded(a.clone()), Excluded(a.clone()))),
                    Just(BytesRange::new(Included(a.clone()), Excluded(a.clone()))),
                    Just(BytesRange::new(Excluded(a.clone()), Included(a.clone()))),
                ]
            })
        ]
    }

    pub(crate) fn nonempty_range(size: usize) -> impl Strategy<Value = BytesRange> {
        prop_oneof![
            Just(BytesRange::new(Unbounded, Unbounded)),
            arbitrary_single_point_bounded_range(size),
            nonempty_partial_bounded_range(size),
            nonempty_bounded_range(size),
            nonempty_range_edge_cases()
        ]
    }
}

#[cfg(test)]
pub(crate) mod rng {
    use proptest::test_runner::{RngAlgorithm, TestRng};
    use rand::Rng;

    pub(crate) fn new_test_rng(seed: Option<[u8; 32]>) -> TestRng {
        let seed = seed.unwrap_or_else(|| {
            let mut thread_rng = rand::thread_rng();
            let random_bytes: [u8; 32] = thread_rng.gen();
            random_bytes
        });
        TestRng::from_seed(RngAlgorithm::ChaCha, &seed)
    }
}

/// Sometimes it is more convenient to generate individual values. This can be
/// useful when we need a sample value which is derived from arbitrary inputs
/// or when the generation of the sample is expensive. For example, it
/// would be too expensive to treat a table of key-value pairs as an arbitrary
/// input into a property test. Instead, we might generate a single arbitrary
/// table sample and use it to seed the database. Then we can run property tests
/// against the database. We nevertheless want the sample table to be deterministically
/// generated so that the test failure can be easily reproduced.
#[cfg(test)]
pub(crate) mod sample {
    use crate::bytes_range;
    use crate::bytes_range::BytesRange;
    use bytes::{BufMut, Bytes, BytesMut};
    use proptest::test_runner::TestRng;
    use rand::distributions::uniform::SampleRange;
    use rand::Rng;
    use std::cmp::max;
    use std::collections::BTreeMap;
    use std::ops::Bound;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    pub(crate) fn bytes<T: SampleRange<usize>>(rng: &mut TestRng, len_range: T) -> Bytes {
        let len = rng.gen_range(len_range);
        let mut v: Vec<u8> = Vec::with_capacity(len);
        for _ in 0..len {
            v.push(rng.gen());
        }
        v.into()
    }

    pub(crate) fn record(rng: &mut TestRng, max_bytes_len: usize) -> (Bytes, Bytes) {
        let key = bytes(rng, 1..max_bytes_len);
        let value = bytes(rng, 0..max_bytes_len);
        (key, value)
    }

    pub(crate) fn table(
        rng: &mut TestRng,
        num_records: usize,
        max_bytes_len: usize,
    ) -> BTreeMap<Bytes, Bytes> {
        let mut table = BTreeMap::new();
        while table.len() < num_records {
            let (key, value) = record(rng, max_bytes_len);
            table.insert(key, value);
        }
        table
    }

    fn bytes_between(rng: &mut TestRng, start: &Bytes, end: &Bytes) -> Bytes {
        assert_eq!(start.len(), end.len());
        let mut res = BytesMut::new();
        let mut start_bound_satisfied = false;
        let mut end_bound_satisfied = false;

        for i in 0..start.len() {
            let next_val_lb = if !start_bound_satisfied {
                start[i]
            } else {
                u8::MIN
            };
            let next_val_ub = if !end_bound_satisfied {
                end[i]
            } else {
                u8::MAX
            };

            let next_val = if next_val_lb == next_val_ub {
                next_val_lb
            } else {
                let next_bound_range_diff = next_val_ub - next_val_lb;
                next_val_lb + (rng.gen::<u8>() % next_bound_range_diff)
            };
            res.put_u8(next_val);

            if next_val > next_val_lb {
                start_bound_satisfied = true;
            }

            if next_val < next_val_ub {
                end_bound_satisfied = true;
            }

            let coin_flip: bool = rng.gen();
            if start_bound_satisfied && end_bound_satisfied && coin_flip {
                break;
            }
        }

        let res = res.freeze();
        assert!(res >= start && res <= end);
        res
    }

    fn increment_lex(b: &Bytes) -> Bytes {
        let mut res = BytesMut::from(b.as_ref());
        for i in (0..res.len()).rev() {
            if res[i] < u8::MAX {
                res[i] += 1;
                return res.freeze();
            } else {
                res[i] = u8::MIN;
            }
        }
        panic!("Overflow when incrementing bytes {b:?}")
    }

    fn decrement_lex(b: &Bytes) -> Bytes {
        let mut res = BytesMut::from(b.as_ref());
        for i in (0..res.len()).rev() {
            if res[i] > u8::MIN {
                res[i] -= 1;
                return res.freeze();
            } else {
                res[i] = u8::MAX;
            }
        }
        panic!("Overflow when decrementing bytes {b:?}")
    }

    fn minvalue_bytes(
        rng: &mut TestRng,
        min_bound: Bound<usize>,
        max_bound: Bound<usize>,
    ) -> Bytes {
        let min_len = match min_bound {
            Unbounded => 0,
            Included(len) => len,
            Excluded(len) => len + 1,
        };

        let max_len = match max_bound {
            Unbounded => min_len + 10,
            Included(len) => len + 1,
            Excluded(len) => len,
        };

        let len = if min_len == max_len {
            min_len
        } else {
            min_len + (rng.gen::<usize>() % (max_len - min_len))
        };

        Bytes::from(vec![u8::MIN; len])
    }

    fn bound_len(bound: Bound<&Bytes>) -> usize {
        match bound {
            Unbounded => 0,
            Included(b) | Excluded(b) => b.len(),
        }
    }

    fn padded_bytes(b: &Bytes, value: u8, len: usize) -> Bytes {
        let mut padded = BytesMut::from(b.as_ref());
        while padded.len() < len {
            padded.put_u8(value);
        }
        padded.freeze()
    }

    fn inclusive_end_bound(range: &BytesRange, len: usize) -> Bytes {
        match range.end_bound() {
            Unbounded => padded_bytes(&Bytes::new(), u8::MAX, len),
            Included(b) | Excluded(b) => {
                let b = padded_bytes(&b, u8::MIN, len);
                decrement_lex(&b)
            }
        }
    }

    fn inclusive_start_bound(range: &BytesRange, len: usize) -> Bytes {
        match range.start_bound() {
            Unbounded => padded_bytes(&Bytes::new(), u8::MIN, len),
            Included(b) => padded_bytes(&b, u8::MIN, len),
            Excluded(b) => {
                let b = padded_bytes(&b, u8::MIN, len);
                increment_lex(&b)
            }
        }
    }

    fn can_decrement_without_truncation(bytes: &Bytes) -> bool {
        bytes.iter().any(|b| *b > u8::MIN)
    }

    pub(crate) fn bytes_in_range(rng: &mut TestRng, range: &BytesRange) -> Bytes {
        assert!(
            range.non_empty(),
            "Cannot choose an arbitrary value from an empty range"
        );

        if let Some(end) = range.end_bound_opt() {
            if !can_decrement_without_truncation(&end) {
                let min_len = range.start_bound().map(|b| b.len());
                let max_len = range.end_bound().map(|b| b.len());
                return minvalue_bytes(rng, min_len, max_len);
            } else if range.start_bound() == Included(&end) {
                return end.clone();
            } else {
                let start = range.start_bound_opt().unwrap_or_else(|| Bytes::new());
                if bytes_range::is_prefix_increment(&start, &end) {
                    return if range.start_bound() == Included(&start) {
                        start.clone()
                    } else {
                        end.clone()
                    };
                }
            }
        }

        let len = max(bound_len(range.start_bound()), bound_len(range.end_bound())) + 3;
        let start_bound = inclusive_start_bound(&range, len);
        let end_bound = inclusive_end_bound(&range, len);
        bytes_between(rng, &start_bound, &end_bound)
    }
}

#[cfg(test)]
mod tests {
    use crate::proptest_util::{arbitrary, sample};
    use proptest::proptest;
    use proptest::test_runner::{RngAlgorithm, TestRng};

    #[test]
    fn test_arbitrary_bytes() {
        proptest!(|(bytes in arbitrary::bytes(10))| {
            assert!(bytes.len() <= 10);
        });

        proptest!(|(bytes in arbitrary::nonempty_bytes(10))| {
            assert!(bytes.len() > 0);
            assert!(bytes.len() <= 10);
        });
    }

    #[test]
    fn test_arbitrary_range() {
        proptest!(|(range in arbitrary::nonempty_range(10))| {
            assert!(range.non_empty());
        });

        proptest!(|(range in arbitrary::empty_range(10))| {
            assert!(range.is_empty());
        });
    }

    #[test]
    fn test_sample_table() {
        let mut rng = TestRng::deterministic_rng(RngAlgorithm::ChaCha);
        let num_records = 50;
        let table = sample::table(&mut rng, num_records, 10);
        assert!(table.len() == num_records);

        for (key, value) in table {
            assert!(key.len() > 0);
            assert!(key.len() <= 10);
            assert!(value.len() <= 10);
        }
    }
}