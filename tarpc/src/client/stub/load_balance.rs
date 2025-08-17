//! Provides load-balancing [Stubs](crate::client::stub::Stub).

pub use consistent_hash::ConsistentHash;
pub use round_robin::RoundRobin;

/// Provides a stub that load-balances with a simple round-robin strategy.
mod round_robin {
    use crate::{
        client::{stub, RpcError},
        context,
    };
    use cycle::AtomicCycle;

    impl<Stub> stub::Stub for RoundRobin<Stub>
    where
        Stub: stub::Stub,
    {
        type Req = Stub::Req;
        type Resp = Stub::Resp;

        async fn call(
            &self,
            ctx: context::Context,
            request: Self::Req,
        ) -> Result<Stub::Resp, RpcError> {
            let next = self.stubs.next();
            next.call(ctx, request).await
        }
    }

    /// A Stub that load-balances across backing stubs by round robin.
    #[derive(Clone, Debug)]
    pub struct RoundRobin<Stub> {
        stubs: AtomicCycle<Stub>,
    }

    impl<Stub> RoundRobin<Stub>
    where
        Stub: stub::Stub,
    {
        /// Returns a new RoundRobin stub.
        pub fn new(stubs: Vec<Stub>) -> Self {
            Self {
                stubs: AtomicCycle::new(stubs),
            }
        }
    }

    mod cycle {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        /// Cycles endlessly and atomically over a collection of elements of type T.
        #[derive(Clone, Debug)]
        pub struct AtomicCycle<T>(Arc<State<T>>);

        #[derive(Debug)]
        struct State<T> {
            elements: Vec<T>,
            next: AtomicUsize,
        }

        impl<T> AtomicCycle<T> {
            pub fn new(elements: Vec<T>) -> Self {
                Self(Arc::new(State {
                    elements,
                    next: Default::default(),
                }))
            }

            pub fn next(&self) -> &T {
                self.0.next()
            }
        }

        impl<T> State<T> {
            pub fn next(&self) -> &T {
                let next = self.next.fetch_add(1, Ordering::Relaxed);
                &self.elements[next % self.elements.len()]
            }
        }

        #[test]
        fn test_cycle() {
            let cycle = AtomicCycle::new(vec![1, 2, 3]);
            assert_eq!(cycle.next(), &1);
            assert_eq!(cycle.next(), &2);
            assert_eq!(cycle.next(), &3);
            assert_eq!(cycle.next(), &1);
        }
    }
}

/// Provides a stub that load-balances with a consistent hashing strategy.
///
/// Each request is hashed, then mapped to a stub based on the hash. Equivalent requests will use
/// the same stub.
mod consistent_hash {
    use crate::{
        client::{stub, RpcError},
        context,
    };
    use std::{
        collections::hash_map::RandomState,
        hash::{BuildHasher, Hash, Hasher},
        num::TryFromIntError,
    };

    impl<Stub, S> stub::Stub for ConsistentHash<Stub, S>
    where
        Stub: stub::Stub,
        Stub::Req: Hash,
        S: BuildHasher,
    {
        type Req = Stub::Req;
        type Resp = Stub::Resp;

        async fn call(
            &self,
            ctx: context::Context,
            request: Self::Req,
        ) -> Result<Stub::Resp, RpcError> {
            let index = usize::try_from(self.hash_request(&request) % self.stubs_len).expect(
                "invariant broken: stubs_len is not larger than a usize, \
                         so the hash modulo stubs_len should always fit in a usize",
            );
            let next = &self.stubs[index];
            next.call(ctx, request).await
        }
    }

    /// A Stub that load-balances across backing stubs by round robin.
    #[derive(Clone, Debug)]
    pub struct ConsistentHash<Stub, S = RandomState> {
        stubs: Vec<Stub>,
        stubs_len: u64,
        hasher: S,
    }

    impl<Stub> ConsistentHash<Stub, RandomState>
    where
        Stub: stub::Stub,
        Stub::Req: Hash,
    {
        /// Returns a new RoundRobin stub.
        /// Returns an err if the length of `stubs` overflows a u64.
        pub fn new(stubs: Vec<Stub>) -> Result<Self, TryFromIntError> {
            Ok(Self {
                stubs_len: stubs.len().try_into()?,
                stubs,
                hasher: RandomState::new(),
            })
        }
    }

    impl<Stub, S> ConsistentHash<Stub, S>
    where
        Stub: stub::Stub,
        Stub::Req: Hash,
        S: BuildHasher,
    {
        /// Returns a new RoundRobin stub.
        /// Returns an err if the length of `stubs` overflows a u64.
        pub fn with_hasher(stubs: Vec<Stub>, hasher: S) -> Result<Self, TryFromIntError> {
            Ok(Self {
                stubs_len: stubs.len().try_into()?,
                stubs,
                hasher,
            })
        }

        fn hash_request(&self, req: &Stub::Req) -> u64 {
            let mut hasher = self.hasher.build_hasher();
            req.hash(&mut hasher);
            hasher.finish()
        }
    }
}
