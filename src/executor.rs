extern crate alloc;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::sync::Arc;
use alloc::task::Wake;
use core::cell::{Cell, RefCell};
use core::fmt::{self, Debug};
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use crossbeam_queue::{ArrayQueue, SegQueue};
use futures::future::LocalBoxFuture;
use futures::task::AtomicWaker;

#[cfg(feature = "std")]
pub fn spawn<F: IntoFuture + 'static>(future: F) -> Task<F::Output> {
	Executor::local(|executor| executor.spawn(future))
}

#[cfg(feature = "std")]
pub fn tick() -> usize {
	Executor::local(|executor| executor.tick())
}

#[cfg(feature = "std")]
pub fn try_tick() -> bool {
	Executor::local(|executor| executor.try_tick())
}

#[cfg(feature = "std")]
pub fn count() -> usize {
	Executor::local(|executor| executor.count())
}

#[cfg(feature = "std")]
pub fn clear() -> usize {
	Executor::local(|executor| executor.clear())
}

#[derive(Default)]
pub struct Executor<'f> {
	tasks: RefCell<BTreeMap<u64, LocalBoxFuture<'f, ()>>>,
	queue: Arc<SegQueue<u64>>,
	next_id: Cell<u64>,
}

impl Executor<'static> {
	#[cfg(feature = "std")]
	pub fn local<T>(func: impl FnOnce(&Self) -> T) -> T {
		thread_local! {
			static EXECUTOR: Executor<'static> = Executor::new();
		}

		EXECUTOR.with(func)
	}
}

impl<'f> Executor<'f> {
	pub fn new() -> Self {
		Self {
			tasks: RefCell::new(BTreeMap::new()),
			queue: Arc::new(SegQueue::new()),
			next_id: Cell::new(0),
		}
	}

	pub fn spawn<F: IntoFuture + 'f>(&self, future: F) -> Task<F::Output> {
		let queue = Arc::new(ArrayQueue::new(1));
		let waker = Arc::new(AtomicWaker::new());
		let queue_clone = queue.clone();
		let waker_clone = waker.clone();
		self.poll_task(self.add_task(async move {
			let _ = queue_clone.push(future.await);
			waker_clone.wake();
		}));
		Task { queue, waker }
	}

	pub fn tick(&self) -> usize {
		let mut count = 0;
		while self.try_tick() {
			count += 1;
		}
		count
	}

	pub fn try_tick(&self) -> bool {
		if let Some(id) = self.queue.pop() {
			self.poll_task(id);
			true
		} else {
			false
		}
	}

	pub fn count(&self) -> usize {
		self.tasks.borrow().len()
	}

	pub fn clear(&self) -> usize {
		let mut tasks = self.tasks.borrow_mut();
		let len = tasks.len();
		tasks.clear();
		len
	}

	fn add_task<F: Future<Output = ()> + 'f>(&self, future: F) -> u64 {
		let id = self.next_id.get();
		self.tasks.borrow_mut().insert(id, Box::pin(future));
		self.next_id.set(id.wrapping_add(1));
		id
	}

	fn poll_task(&self, id: u64) {
		struct TaskWake {
			queue: Arc<SegQueue<u64>>,
			id: u64,
		}

		impl Wake for TaskWake {
			fn wake_by_ref(self: &Arc<Self>) {
				let _ = self.queue.push(self.id);
			}

			fn wake(self: Arc<Self>) {
				self.wake_by_ref();
			}
		}

		let task = self.tasks.borrow_mut().remove(&id);
		if let Some(mut task) = task {
			let queue = self.queue.clone();
			let waker = Waker::from(Arc::new(TaskWake { queue, id }));
			let mut context = Context::from_waker(&waker);
			if task.as_mut().poll(&mut context).is_pending() {
				self.tasks.borrow_mut().insert(id, task);
			}
		}
	}
}

impl<'f> Debug for Executor<'f> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Executor")
			.field("queue", &self.queue)
			.field("next_id", &self.next_id.get())
			.finish()
	}
}

#[derive(Debug)]
pub struct Task<T> {
	queue: Arc<ArrayQueue<T>>,
	waker: Arc<AtomicWaker>,
}

impl<T> Future for Task<T> {
	type Output = T;

	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.waker.register(cx.waker());
		match self.queue.pop() {
			Some(value) => Poll::Ready(value),
			None => Poll::Pending,
		}
	}
}
