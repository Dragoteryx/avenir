use core::pin::Pin;
use core::task::{Context, Poll};

#[repr(transparent)]
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct YieldNow {
	ready: bool,
}

pub const fn yield_now() -> YieldNow {
	YieldNow { ready: false }
}

impl Future for YieldNow {
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
		if self.ready {
			Poll::Ready(())
		} else {
			self.ready = true;
			cx.waker().wake_by_ref();
			Poll::Pending
		}
	}
}
