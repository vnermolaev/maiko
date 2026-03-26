use std::fmt;
use std::sync::{Arc, RwLock};

use dashmap::DashMap;

use crate::{ActorId, Envelope, Event, OverflowPolicy, Topic, monitoring::Monitor};

/// Monitor that tracks actor lifecycle and overflow counts.
///
/// Register with the supervisor to passively observe actor registration,
/// shutdown, and overflow events. Query at any time from any thread.
///
/// ```ignore
/// let monitor = ActorMonitor::new();
/// let query = monitor.clone();
/// sup.monitors().add(monitor).await;
///
/// // Later, from any thread:
/// let alive = query.is_alive(&actor_id);
/// let overflows = query.overflow_count(&actor_id);
/// ```
#[derive(Clone)]
pub struct ActorMonitor {
    actors: Arc<DashMap<ActorId, RwLock<ActorStats>>>,
}

struct ActorStats {
    stopped: bool,
    overflow_count: usize,
}

impl ActorStats {
    fn new() -> Self {
        Self {
            stopped: false,
            overflow_count: 0,
        }
    }
}

impl ActorMonitor {
    /// Create a new `ActorMonitor`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            actors: Arc::new(DashMap::new()),
        }
    }

    /// Returns a snapshot of currently active actor IDs.
    pub fn actors(&self) -> Vec<ActorId> {
        self.actors
            .iter()
            .filter_map(|entry| {
                let stats = entry.value().read().unwrap();
                (!stats.stopped).then(|| entry.key().clone())
            })
            .collect()
    }

    /// Returns a snapshot of stopped actor IDs.
    pub fn stopped_actors(&self) -> Vec<ActorId> {
        self.actors
            .iter()
            .filter_map(|entry| {
                let stats = entry.value().read().unwrap();
                stats.stopped.then(|| entry.key().clone())
            })
            .collect()
    }

    /// Returns `true` if the actor is currently active.
    pub fn is_alive(&self, actor: &ActorId) -> bool {
        self.actors
            .get(actor)
            .map(|entry| !entry.value().read().unwrap().stopped)
            .unwrap_or(false)
    }

    /// Returns `true` if the actor was registered and has since stopped.
    ///
    /// Returns `false` for actors that were never registered or are still active.
    pub fn is_stopped(&self, actor: &ActorId) -> bool {
        self.actors
            .get(actor)
            .map(|entry| entry.value().read().unwrap().stopped)
            .unwrap_or(false)
    }

    /// Returns the number of overflow events observed for this actor.
    pub fn overflow_count(&self, actor: &ActorId) -> usize {
        self.actors
            .get(actor)
            .map(|entry| entry.value().read().unwrap().overflow_count)
            .unwrap_or(0)
    }
}

impl<E, T> Monitor<E, T> for ActorMonitor
where
    E: Event,
    T: Topic<E> + Send,
{
    fn on_actor_registered(&self, actor_id: &ActorId) {
        let entry = self
            .actors
            .entry(actor_id.clone())
            .or_insert_with(|| RwLock::new(ActorStats::new()));
        let mut stats = entry.write().unwrap();
        stats.stopped = false;
    }

    fn on_actor_stop(&self, actor_id: &ActorId) {
        let entry = self
            .actors
            .entry(actor_id.clone())
            .or_insert_with(|| RwLock::new(ActorStats::new()));
        let mut stats = entry.write().unwrap();
        stats.stopped = true;
    }

    fn on_overflow(
        &self,
        _envelope: &Envelope<E>,
        _topic: &T,
        receiver: &ActorId,
        _policy: OverflowPolicy,
    ) {
        let entry = self
            .actors
            .entry(receiver.clone())
            .or_insert_with(|| RwLock::new(ActorStats::new()));
        let mut stats = entry.write().unwrap();
        stats.overflow_count += 1;
    }
}

impl Default for ActorMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ActorMonitor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let active = self.actors().len();
        let stopped = self.stopped_actors().len();
        let overflows = self
            .actors
            .iter()
            .filter(|entry| entry.value().read().unwrap().overflow_count > 0)
            .count();
        f.debug_struct("ActorMonitor")
            .field("active", &active)
            .field("stopped", &stopped)
            .field("overflows", &overflows)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::DefaultTopic;

    use super::*;

    #[derive(Clone, Debug)]
    struct TestEvent;
    impl Event for TestEvent {}

    fn make_id(name: &str) -> ActorId {
        ActorId::new(name)
    }

    #[test]
    fn default_is_empty() {
        let m = ActorMonitor::default();
        assert!(m.actors().is_empty());
        assert!(m.stopped_actors().is_empty());
    }

    #[test]
    fn registered_actor_is_alive() {
        let monitor = ActorMonitor::new();
        let a = make_id("actor-1");
        let m: &dyn Monitor<TestEvent, DefaultTopic> = &monitor;
        m.on_actor_registered(&a);

        assert!(monitor.is_alive(&a));
        assert!(monitor.actors().contains(&a));
    }

    #[test]
    fn stopped_actor_is_not_alive() {
        let monitor = ActorMonitor::new();
        let a = make_id("actor-2");
        let m: &dyn Monitor<TestEvent, DefaultTopic> = &monitor;
        m.on_actor_registered(&a);
        m.on_actor_stop(&a);

        assert!(!monitor.is_alive(&a));
        assert!(monitor.stopped_actors().contains(&a));
    }

    #[test]
    fn overflow_count_increments() {
        let monitor = ActorMonitor::new();
        let a = make_id("actor-3");
        let env = Envelope::new(TestEvent, a.clone());
        let topic = DefaultTopic;

        assert_eq!(monitor.overflow_count(&a), 0);
        monitor.on_overflow(&env, &topic, &a, OverflowPolicy::Fail);
        assert_eq!(monitor.overflow_count(&a), 1);
        monitor.on_overflow(&env, &topic, &a, OverflowPolicy::Fail);
        assert_eq!(monitor.overflow_count(&a), 2);
    }

    #[test]
    fn overflow_and_lifecycle_are_independent() {
        let monitor = ActorMonitor::new();
        let a = make_id("actor-4");
        let env = Envelope::new(TestEvent, a.clone());
        let topic = DefaultTopic;

        let m: &dyn Monitor<TestEvent, DefaultTopic> = &monitor;
        m.on_actor_registered(&a);
        monitor.on_overflow(&env, &topic, &a, OverflowPolicy::Fail);

        assert!(monitor.is_alive(&a));
        assert_eq!(monitor.overflow_count(&a), 1);

        m.on_actor_stop(&a);
        assert!(!monitor.is_alive(&a));
        assert_eq!(monitor.overflow_count(&a), 1);
    }

    #[test]
    fn unknown_actor_is_not_alive() {
        let monitor = ActorMonitor::new();
        let a = make_id("unknown");
        assert!(!monitor.is_alive(&a));
        assert_eq!(monitor.overflow_count(&a), 0);
    }

    #[test]
    fn clone_shares_state() {
        let monitor = ActorMonitor::new();
        let query = monitor.clone();
        let a = make_id("actor-5");

        let m: &dyn Monitor<TestEvent, DefaultTopic> = &monitor;
        m.on_actor_registered(&a);

        assert!(query.is_alive(&a));
    }
}
