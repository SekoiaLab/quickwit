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
/// `Bytes` dereferences to a heap-allocated slice whose address does not change when the `Bytes`
/// itself is moved, which is exactly the contract `StableDeref` requires.
pub(crate) struct StableDerefBytes(pub Bytes);

impl Deref for StableDerefBytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

// SAFETY: `Bytes` stores its payload behind a stable heap pointer; moving the `Bytes` (and thus
// the `StableDerefBytes`) does not invalidate the slice returned by `deref`.
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

        // Move it as a value.
        let stable_deref_bytes = black_box(stable_deref_bytes);
        assert_eq!(stable_deref_bytes.deref().as_ptr(), address);

        // Move it onto the heap, then back off it.
        let boxed = Box::new(stable_deref_bytes);
        assert_eq!(boxed.deref().as_ptr(), address);
        let stable_deref_bytes = *boxed;

        // Move it inside a container that reallocates under it.
        let mut relocating = vec![stable_deref_bytes];
        for _ in 0..16 {
            relocating.push(StableDerefBytes(Bytes::new()));
        }
        assert_eq!(relocating[0].deref().as_ptr(), address);
    }

    /// Guards the `unsafe impl StableDeref for StableDerefBytes`.
    ///
    /// That impl rests on a `Bytes` never holding its payload inline, which is an
    /// implementation detail of the `bytes` crate rather than something its API
    /// promises (it does not implement `StableDeref` itself). Were a version to ever
    /// grow an inline representation for short payloads, the impl would become
    /// unsound with no compilation error whatsoever; this is what turns that into a
    /// failing test instead.
    #[test]
    fn test_stable_deref_bytes_address_survives_moves() {
        assert_deref_survives_moves(Bytes::from_static(b"a static payload"));
        assert_deref_survives_moves(Bytes::from(b"a heap allocated payload".to_vec()));
        // How the segments of a response body reach us.
        assert_deref_survives_moves(BytesMut::from(&b"a downloaded segment"[..]).freeze());
        // A short payload, which is what an inline representation would target.
        assert_deref_survives_moves(Bytes::from(b"ab".to_vec()));
        // A slice of a larger buffer, and an empty one.
        assert_deref_survives_moves(Bytes::from(b"a sliced payload".to_vec()).slice(2..8));
        assert_deref_survives_moves(Bytes::new());
    }

    #[test]
    fn test_into_owned_bytes_preserves_the_payload() {
        let owned_bytes = into_owned_bytes(Bytes::from_static(b"a downloaded payload"));
        assert_eq!(owned_bytes.as_slice(), b"a downloaded payload");
    }
}
