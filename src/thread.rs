use crossbeam_queue::ArrayQueue;
use futures::task::AtomicWaker;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread::{JoinHandle, spawn};

#[derive(Debug)]
pub struct Blocking<T> {
	queue: Arc<ArrayQueue<Result<T, Box<dyn Any + Send>>>>,
	waker: Arc<AtomicWaker>,
	thread: Option<JoinHandle<()>>,
}

pub fn blocking<F, T>(func: F) -> Blocking<T>
where
	F: FnOnce() -> T + Send + 'static,
	T: Send + 'static,
{
	let queue = Arc::new(ArrayQueue::new(1));
	let waker = Arc::new(AtomicWaker::new());
	let queue_clone = queue.clone();
	let waker_clone = waker.clone();
	let thread = Some(spawn(move || {
		let func = AssertUnwindSafe(func);
		let _ = queue_clone.push(catch_unwind(func));
		waker_clone.wake();
	}));

	Blocking {
		queue,
		waker,
		thread,
	}
}

impl<T> Blocking<T> {
	#[inline]
	pub fn detach(mut self) -> Self {
		self.thread.take();
		self
	}
}

impl<T> Future for Blocking<T> {
	type Output = T;

	fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		self.waker.register(cx.waker());
		match self.queue.pop() {
			Some(Ok(value)) => Poll::Ready(value),
			Some(Err(err)) => resume_unwind(err),
			None => Poll::Pending,
		}
	}
}

impl<T> Drop for Blocking<T> {
	fn drop(&mut self) {
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
	}
}
