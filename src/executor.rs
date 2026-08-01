extern crate alloc;

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::task::Wake;
use core::cell::{Cell, RefCell};
use core::fmt::{self, Debug};
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use crossbeam_queue::SegQueue;
use futures_channel::oneshot::*;
use futures_core::future::LocalBoxFuture;
use hashbrown::HashMap;
use nohash_hasher::BuildNoHashHasher;

#[cfg(feature = "std")]
pub fn spawn<F: IntoFuture + 'static>(future: F) -> Task<F::Output> {
	Executor::local(|executor| executor.spawn(future))
}

#[cfg(feature = "std")]
pub fn tick() -> usize {
	Executor::local(Executor::tick)
}

#[cfg(feature = "std")]
pub fn try_tick() -> bool {
	Executor::local(Executor::try_tick)
}

#[cfg(feature = "std")]
pub fn tasks() -> usize {
	Executor::local(Executor::tasks)
}

#[cfg(feature = "std")]
pub fn scheduled() -> usize {
	Executor::local(Executor::scheduled)
}

#[cfg(feature = "std")]
pub fn cancelled() -> usize {
	Executor::local(Executor::cancelled)
}

#[cfg(feature = "std")]
pub fn clear() -> usize {
	Executor::local(Executor::clear)
}

#[derive(Default)]
pub struct Executor<'f> {
	tasks: RefCell<HashMap<u64, LocalBoxFuture<'f, ()>, BuildNoHashHasher<u64>>>,
	scheduled: Arc<SegQueue<u64>>,
	cancelled: Arc<SegQueue<u64>>,
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
		Self::default()
	}

	pub fn tasks(&self) -> usize {
		self.tasks.borrow().len()
	}

	pub fn scheduled(&self) -> usize {
		self.scheduled.len()
	}

	pub fn cancelled(&self) -> usize {
		self.cancelled.len()
	}

	pub fn spawn<F: IntoFuture + 'f>(&self, future: F) -> Task<F::Output> {
		let cancelled = Some(self.cancelled.clone());
		let (sender, receiver) = channel();
		let id = self.add_task(async move {
			let _ = sender.send(future.await);
		});

		self.poll_task(id);
		Task {
			receiver,
			cancelled,
			id,
		}
	}

	pub fn tick(&self) -> usize {
		let mut count = 0;
		while self.try_tick() {
			count += 1;
		}
		count
	}

	pub fn try_tick(&self) -> bool {
		self.drop_cancelled();

		if let Some(id) = self.scheduled.pop() {
			self.poll_task(id);
			true
		} else {
			false
		}
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
		struct WakeTask {
			scheduled: Arc<SegQueue<u64>>,
			woken: AtomicBool,
			id: u64,
		}

		impl Wake for WakeTask {
			fn wake_by_ref(self: &Arc<Self>) {
				if !self.woken.swap(true, Ordering::AcqRel) {
					let _ = self.scheduled.push(self.id);
				}
			}

			fn wake(self: Arc<Self>) {
				self.wake_by_ref();
			}
		}

		let task = self.tasks.borrow_mut().remove(&id);
		if let Some(mut task) = task {
			let wake_task = WakeTask {
				scheduled: self.scheduled.clone(),
				woken: AtomicBool::new(false),
				id,
			};

			let waker = Waker::from(Arc::new(wake_task));
			let mut context = Context::from_waker(&waker);
			if task.as_mut().poll(&mut context).is_pending() {
				self.tasks.borrow_mut().insert(id, task);
			}
		}
	}

	fn drop_cancelled(&self) {
		if !self.cancelled.is_empty() {
			let mut tasks = self.tasks.borrow_mut();
			while let Some(id) = self.cancelled.pop() {
				tasks.remove(&id);
			}
		}
	}
}

impl<'f> Debug for Executor<'f> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Executor")
			.field("tasks", &self.tasks())
			.field("scheduled", &self.scheduled())
			.field("cancelled", &self.cancelled())
			.finish()
	}
}

#[derive(Debug)]
pub struct Task<T> {
	cancelled: Option<Arc<SegQueue<u64>>>,
	receiver: Receiver<T>,
	id: u64,
}

impl<T> Task<T> {
	#[inline]
	pub fn detach(mut self) -> Self {
		self.cancelled.take();
		self
	}
}

impl<T> Future for Task<T> {
	type Output = T;

	#[track_caller]
	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		match Pin::new(&mut self.receiver).poll(cx) {
			Poll::Ready(Err(_)) => panic!("Task has been dropped"),
			Poll::Ready(Ok(value)) => Poll::Ready(value),
			Poll::Pending => Poll::Pending,
		}
	}
}

impl<T> Drop for Task<T> {
	fn drop(&mut self) {
		if let Some(cancelled) = self.cancelled.take() {
			let _ = cancelled.push(self.id);
		}
	}
}
