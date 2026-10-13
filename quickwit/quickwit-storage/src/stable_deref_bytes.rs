// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Deref;

use bytes::Bytes;
use stable_deref_trait::StableDeref;

use crate::OwnedBytes;

/// `StableDeref` wrapper around [`Bytes`] so it can back an [`OwnedBytes`] without an extra copy.
///
/// A `Bytes` is a `(ptr, len, data, vtable)` handle whose `ptr` points into a separate allocation
/// (or into static memory), never into the handle itself, and `deref` just rebuilds a slice from
/// that pointer. Moving the handle around therefore leaves the payload where it is, which is what
/// `StableDeref` asks for.
struct StableDerefBytes(Bytes);

impl Deref for StableDerefBytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

// SAFETY: `StableDeref` asks for two things, and `Bytes` gives both.
//
// The address must survive a move of the value: it does, because the payload lives in an
// allocation of its own that the handle only points at (see the type documentation above).
//
// The address must also survive arbitrary `&self` methods. This is the clause that is not free,
// because `Bytes` does mutate itself through a shared reference: cloning a `Bytes` that is still
// backed by a `Vec` promotes it to an `Arc` by swapping its `data` pointer. That promotion only
// wraps the buffer that is already there in a refcount, without reallocating or copying it, so the
// payload address is untouched.
//
// Nothing is required of the `&mut self` methods that *do* move the pointer (`advance`,
// `split_to`), and they are out of reach anyway: this type exposes no `DerefMut`, and `OwnedBytes`
// keeps it behind an `Arc`.
unsafe impl StableDeref for StableDerefBytes {}

/// Wraps a [`Bytes`] into an [`OwnedBytes`] without copying its contents.
#[inline]
pub(crate) fn into_owned_bytes(bytes: Bytes) -> OwnedBytes {
    OwnedBytes::new(StableDerefBytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;

    use bytes::BytesMut;

    use super::*;

    /// Moves a `StableDerefBytes` around in the ways that would relocate it in memory
    /// and checks that the address its `Deref` returns never budges.
    #[track_caller]
    fn assert_deref_survives_moves(payload: Bytes) {
        let stable_deref_bytes = StableDerefBytes(payload);
        let address = stable_deref_bytes.deref().as_ptr();

        // Pass it by value. The compiler is free to leave it in the same stack slot, so this
        // step proves the least of the three; `black_box` only keeps it from being optimized
        // away entirely.
        let stable_deref_bytes = black_box(stable_deref_bytes);
        assert_eq!(stable_deref_bytes.deref().as_ptr(), address);

        // Move it onto the heap, then back off it. This one really does relocate the handle.
        let boxed = Box::new(stable_deref_bytes);
        assert_eq!(boxed.deref().as_ptr(), address);
        let stable_deref_bytes = *boxed;

        // Push past the vec's capacity so it reallocates and memcpies the handle elsewhere.
        let mut relocating = vec![stable_deref_bytes];
        for _ in 0..16 {
            relocating.push(StableDerefBytes(Bytes::new()));
        }
        assert_eq!(relocating[0].deref().as_ptr(), address);
    }

    /// Guards the `unsafe impl StableDeref for StableDerefBytes`.
    ///
    /// No `Bytes` of the current `bytes` release can fail this test: the
    /// payload is never held inside the handle, so there is nothing a move
    /// could relocate. That is precisely why the test is here. Holding the
    /// payload out of line is an implementation detail of the crate rather than
    /// something its API promises (it does not implement `StableDeref` itself),
    /// so were a future version to grow an inline representation for short
    /// payloads, our impl would silently become unsound. This test tries to
    /// catch such regressions.
    #[test]
    fn test_stable_deref_bytes_address_survives_moves() {
        assert_deref_survives_moves(Bytes::from_static(b"a static payload"));
        assert_deref_survives_moves(Bytes::from(b"a heap allocated payload".to_vec()));
        // How the segments of a response body reach us.
        assert_deref_survives_moves(BytesMut::from(&b"a downloaded segment"[..]).freeze());
        // A short payload, the one an inline representation would store in the handle.
        assert_deref_survives_moves(Bytes::from(b"ab".to_vec()));
        // A view into a larger buffer, and an empty payload.
        assert_deref_survives_moves(Bytes::from(b"a sliced payload".to_vec()).slice(2..8));
        assert_deref_survives_moves(Bytes::new());
    }

    #[test]
    fn test_into_owned_bytes_preserves_the_payload() {
        let owned_bytes = into_owned_bytes(Bytes::from_static(b"a downloaded payload"));
        assert_eq!(owned_bytes.as_slice(), b"a downloaded payload");
    }
}
