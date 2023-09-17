use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::{Context, Poll, Waker},
};

use futures_core::Stream;

pub struct PubSub<T: Clone> {
    channel: Arc<RwLock<Channel<T>>>,
}

impl<T: Clone> PubSub<T> {
    pub fn new() -> Self {
        Self {
            channel: Arc::new(RwLock::new(Channel {
                publishers: 1,
                subscribers: HashMap::new(),
                ids: VecDeque::new(),
                closed: false,
            })),
        }
    }

    pub fn send(&self, item: T) {
        let channel = self.channel.read().unwrap();

        for state in channel.subscribers.values() {
            let mut s = state.lock().unwrap();
            s.buffer.push_back(item.clone());
            if let Some(w) = s.waker.clone() {
                w.wake();
            }
        }
    }

    pub fn subscribe(&self) -> Subscriber<T> {
        subscribe(self.channel.clone())
    }

    pub(crate) fn len(&self) -> usize {
        self.channel.read().unwrap().subscribers.len()
    }
}

impl<T: Clone> Clone for PubSub<T> {
    fn clone(&self) -> Self {
        let mut channel = self.channel.write().unwrap();
        channel.publishers += 1;
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<T: Clone> Drop for PubSub<T> {
    fn drop(&mut self) {
        let mut channel = self.channel.write().unwrap();
        channel.closed = true;
        channel.publishers -= 1;

        if channel.publishers > 0 {
            return;
        }

        for state in channel.subscribers.values() {
            let mut s = state.lock().unwrap();
            s.closed = true;
            if let Some(w) = s.waker.clone() {
                w.wake();
            }
        }
    }
}

fn subscribe<T: Clone>(channel_lock: Arc<RwLock<Channel<T>>>) -> Subscriber<T> {
    let mut channel = channel_lock.write().unwrap();
    let id = {
        if let Some(id) = channel.ids.pop_front() {
            id
        } else {
            channel.subscribers.len()
        }
    };

    let state = Arc::new(Mutex::new(SubscriptionState {
        buffer: VecDeque::new(),
        waker: None,
        closed: channel.closed,
    }));

    channel.subscribers.insert(id, state.clone());

    Subscriber {
        channel: channel_lock.clone(),
        id,
        state,
    }
}

struct Channel<T: Clone> {
    publishers: usize,
    closed: bool,
    subscribers: HashMap<usize, Arc<Mutex<SubscriptionState<T>>>>,
    ids: VecDeque<usize>,
}

impl<T: Clone> Channel<T> {
    fn unsubscribe(&mut self, id: usize) {
        self.subscribers.remove(&id);
        self.ids.push_back(id);
    }
}

struct SubscriptionState<T: Clone> {
    buffer: VecDeque<T>,
    waker: Option<Waker>,
    closed: bool,
}

pub struct Subscriber<T: Clone> {
    channel: Arc<RwLock<Channel<T>>>,
    id: usize,
    state: Arc<Mutex<SubscriptionState<T>>>,
}

impl<T: Clone> Clone for Subscriber<T> {
    fn clone(&self) -> Self {
        subscribe(self.channel.clone())
    }
}

impl<T: Clone> Stream for Subscriber<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.state.lock().unwrap();
        if let Some(item) = state.buffer.pop_front() {
            state.waker = None;
            return Poll::Ready(Some(item));
        }

        if state.closed {
            return Poll::Ready(None);
        }

        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T: Clone> Drop for Subscriber<T> {
    fn drop(&mut self) {
        self.channel.write().unwrap().unsubscribe(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    use futures_core::Stream;
    use futures_util::{task::noop_waker_ref, StreamExt};

    #[tokio::test]
    async fn zero_subscribers() {
        let pubsub = super::PubSub::<usize>::new();

        pubsub.send(1);
        pubsub.send(2);
    }

    #[tokio::test]
    async fn single_subscriber() {
        let pubsub = super::PubSub::<usize>::new();
        let mut subscriber = pubsub.subscribe();

        pubsub.send(1);
        pubsub.send(2);

        assert_eq!(subscriber.next().await, Some(1));
        assert_eq!(subscriber.next().await, Some(2));

        drop(pubsub);

        assert_eq!(subscriber.next().await, None);
    }

    #[tokio::test]
    async fn multiple_subscriber() {
        let pubsub = super::PubSub::<usize>::new();
        let mut subscriber1 = pubsub.subscribe();
        let mut subscriber2 = pubsub.subscribe();

        pubsub.send(1);
        pubsub.send(2);

        assert_eq!(subscriber1.next().await, Some(1));
        assert_eq!(subscriber2.next().await, Some(1));
        assert_eq!(subscriber1.next().await, Some(2));
        assert_eq!(subscriber2.next().await, Some(2));

        drop(pubsub);

        assert_eq!(subscriber1.next().await, None);
        assert_eq!(subscriber2.next().await, None);
    }

    #[tokio::test]
    async fn subscriber_map_single_subscriber() {
        let pubsub = super::PubSub::<usize>::new();
        let mut subscriber = pubsub.subscribe().map(|x| x * 2);

        pubsub.send(1);
        pubsub.send(2);

        assert_eq!(subscriber.next().await, Some(2));
        assert_eq!(subscriber.next().await, Some(4));

        drop(pubsub);

        assert_eq!(subscriber.next().await, None);
    }

    #[tokio::test]
    async fn subscriber_map_multiple_subscriber() {
        let pubsub = super::PubSub::<usize>::new();
        let mut subscriber1 = pubsub.subscribe();
        let mut subscriber2 = pubsub.subscribe().map(|x| x * 2);
        let mut subscriber3 = pubsub.subscribe().map(|x| x * 3);

        pubsub.send(1);
        pubsub.send(2);

        assert_eq!(subscriber1.next().await, Some(1));
        assert_eq!(subscriber2.next().await, Some(2));
        assert_eq!(subscriber3.next().await, Some(3));

        assert_eq!(subscriber1.next().await, Some(2));
        assert_eq!(subscriber2.next().await, Some(4));
        assert_eq!(subscriber3.next().await, Some(6));

        drop(pubsub);

        assert_eq!(subscriber1.next().await, None);
        assert_eq!(subscriber2.next().await, None);
        assert_eq!(subscriber3.next().await, None);
    }

    #[tokio::test]
    async fn multiple_publisher() {
        let pubsub = super::PubSub::<usize>::new();
        let mut subscriber = pubsub.subscribe();
        let clone = pubsub.clone();

        pubsub.send(1);
        clone.send(2);

        assert_eq!(subscriber.next().await, Some(1));
        assert_eq!(subscriber.next().await, Some(2));

        drop(pubsub);
        assert_eq!(
            Pin::new(&mut subscriber).poll_next(&mut Context::from_waker(noop_waker_ref())),
            Poll::Pending
        );
        drop(clone);
        assert_eq!(
            Pin::new(&mut subscriber).poll_next(&mut Context::from_waker(noop_waker_ref())),
            Poll::Ready(None),
        );
    }

    #[test]
    fn unsubscribe() {
        let pubsub = super::PubSub::<usize>::new();

        assert_eq!(pubsub.len(), 0);

        let subscriber1 = pubsub.subscribe();

        assert_eq!(pubsub.len(), 1);

        let subscriber2 = pubsub.subscribe().map(|x| x * 2);

        assert_eq!(pubsub.len(), 2);

        drop(subscriber1);

        assert_eq!(pubsub.len(), 1);

        drop(subscriber2);

        assert_eq!(pubsub.len(), 0);
    }
}
