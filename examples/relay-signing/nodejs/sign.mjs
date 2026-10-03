/**
 * Pocket Network relay ring signature (bLSAG) — a signer for Node.js.
 *
 * A Pocket relay is authorised by a 2-member ring signature over
 * [applicationPubKey, gatewayPubKey], in exactly that order, signed by the
 * gateway. The authority on whether a signature is valid is the Go verifier the
 * relayer runs (github.com/pokt-network/ring-go). This file is a translation of
 * the Go signer in ../oracle, and `verify-against-oracle.mjs` checks it by
 * handing every signature it produces to that same Go verifier.
 *
 * Three things in here are surprising, and all three are load-bearing:
 *
 *   1. hashToScalar left-aligns a short reduction (the quirk). ~1 in 256.
 *   2. SHA-3 means FIPS-202, not Keccak. The libraries ship both.
 *   3. encodeScalar (the wire) right-aligns — the exact opposite of (1).
 *
 * Each is commented where it happens. If you are porting this to another
 * language, those comments are the whole point of the file.
 */

// FIPS-202 SHA-3. @noble/hashes also exports keccak_256/keccak_512, which are
// the ORIGINAL Keccak submission: same sponge, different padding, completely
// different digests. Pocket uses FIPS-202. Importing the keccak_* twins here
// type-checks, runs, produces plausible 32/64-byte digests, and every signature
// you make is rejected. The harness pins this with a NIST known-answer test.
import { sha3_256, sha3_512 } from '@noble/hashes/sha3.js';
import { secp256k1 } from '@noble/curves/secp256k1.js';
import {
  bytesToNumberBE,
  concatBytes,
  numberToBytesBE,
  numberToVarBytesBE,
  randomBytes,
} from '@noble/curves/utils.js';

const Point = secp256k1.Point;
const G = Point.BASE;

/** Order of the secp256k1 group. Scalars live in [1, N). */
const N = Point.Fn.ORDER; // fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141
/** Prime of the secp256k1 base field. Coordinates live in [0, P). */
const P = Point.Fp.ORDER; // fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f

/**
 * Euclidean modulo.
 *
 * JavaScript's `%` keeps the sign of the dividend: `-1n % N` is `-1n`, not
 * `N - 1n`. Go's big.Int.Mod is Euclidean and never returns a negative. The
 * signer computes `u - c*x`, which goes negative about half the time, so a bare
 * `%` there produces a negative scalar and roughly every second signature is
 * rejected. Not the same bug as the quirk below, but it looks identical from
 * the outside.
 */
const mod = (a, m = N) => ((a % m) + m) % m;

/**
 * hashToScalar: SHA3-512 the input, reduce mod N, re-encode with go-dleq's
 * quirk. Mirrors go-dleq secp256k1/curve_decred.go HashToScalar.
 *
 * THE QUIRK — this is the one that costs you relays. Go does:
 *
 *     var reduced [32]byte
 *     copy(reduced[:], n.Bytes())
 *
 * `n.Bytes()` is the MINIMAL big-endian encoding of n (31 bytes when n < 2^248,
 * 30 when n < 2^240, ...) and `copy` writes from offset 0. So a short n lands
 * LEFT-aligned in the 32-byte buffer, and reading it back gives n * 256^k for k
 * missing bytes — not n. The natural port zero-pads on the left instead
 * (numberToBytesBE), which is a different scalar for exactly those inputs.
 *
 * A short reduction happens ~1 input in 256. A signature over a 2-member ring
 * derives 2 challenges, so a right-aligning port builds a signature the relayer
 * rejects ~1 time in 128 (~0.78%): rare enough to look like a flaky network,
 * frequent enough to lose real money. This is proven, not theoretical — the
 * oracle pins it in TestCanonicalHashToScalarIsRejected, and this repo's
 * harness reproduces the rate as a negative control.
 *
 * @param {Uint8Array} input
 * @returns {bigint} scalar in [0, N)
 */
export function hashToScalar(input) {
  const n = bytesToNumberBE(sha3_512(input)) % N;

  const reduced = new Uint8Array(32);
  reduced.set(numberToVarBytesBE(n), 0); // Go: copy(reduced[:], n.Bytes())
  // ^ numberToVarBytesBE is the minimal encoding, written at offset 0, so a
  //   short n is LEFT-aligned. THIS IS THE QUIRK — do not "fix" it to
  //   numberToBytesBE(n, 32). See encodeScalar for where that IS correct.

  // Go leaves this value unreduced and reduces at each use site instead (every
  // scalar multiplication, and the final encode). Left-aligning can push it
  // past N, so we fold that reduction in here: equivalent, and it keeps every
  // caller's scalar inside the range noble's multiply() will accept.
  return mod(bytesToNumberBE(reduced));
}

/**
 * hashToCurve: map a compressed public key onto a curve point by
 * try-and-increment. Mirrors ring-go helpers.go hashToCurveSecp256k1.
 *
 * SHA3-256 the key, read the digest as a field element X, and take the point on
 * y^2 = x^3 + 7 with EVEN Y. Only about half of all X values have a point at
 * all; on a miss it re-hashes THE DIGEST — not the original public key — and
 * tries again.
 *
 * The 33-byte check is the same one buildRelaySignature makes, for the same
 * reason: this function hashes the input BYTES, so handing it a 65-byte
 * uncompressed encoding of the very same point returns a different H(P) —
 * silently, with no error and no diagnostic downstream.
 *
 * @param {Uint8Array} pubKey33 compressed SEC1 public key
 * @returns {Point}
 */
export function hashToCurve(pubKey33) {
  if (!(pubKey33 instanceof Uint8Array) || pubKey33.length !== 33) {
    throw new Error(
      `hashToCurve needs a 33-byte compressed SEC1 key, got ` +
        `${pubKey33?.length ?? 0} bytes of ${pubKey33?.constructor?.name ?? typeof pubKey33}` +
        (pubKey33?.length === 65 ? ' (that is an UNCOMPRESSED key; compress it first)' : ''),
    );
  }
  let digest = sha3_256(pubKey33);

  for (let i = 0; i < 128; i++) {
    // Go reads the digest into a field value, whose arithmetic is mod P, so a
    // digest >= P would act as digest-P. Reducing here matches that exactly.
    // (P is within 2^32 of 2^256, so this only bites ~1 digest in 2^224.)
    const x = numberToBytesBE(bytesToNumberBE(digest) % P, 32);

    try {
      // SEC1 prefix 0x02 means "the point with this X and the EVEN Y" — exactly
      // what Go asks for with DecompressY(x, /*odd=*/false). fromBytes throws
      // when no point has this X, which is Go's `DecompressY(...) == false`.
      return Point.fromBytes(concatBytes(Uint8Array.of(0x02), x));
    } catch {
      digest = sha3_256(digest); // re-hash the DIGEST, not the pubkey
    }
  }
  throw new Error('hashToCurve: no valid point found in 128 attempts');
}

/**
 * encodeScalar: the WIRE encoding — 32 bytes big-endian, zero-padded on the
 * LEFT. Mirrors Go's big.Int.FillBytes.
 *
 * Note the contrast with hashToScalar: scalars on the wire are canonical, and
 * only the challenge derivation left-aligns. Two encodings of the same type,
 * 60 lines apart, deliberately opposite. Applying the quirk here — or the
 * canonical form up there — breaks signing.
 *
 * @param {bigint} n
 * @returns {Uint8Array} 32 bytes
 */
export function encodeScalar(n) {
  return numberToBytesBE(mod(n), 32);
}

/**
 * A uniform scalar in [1, N).
 *
 * Rejection sampling rather than `randomBytes(32) mod N`, which biases the low
 * end (negligibly for secp256k1, but the loop costs nothing). Go's
 * rand.Int(rand.Reader, N) samples [0, N); we exclude 0 because noble's
 * multiply() rejects a zero scalar — and a zero nonce would leak the private
 * key anyway.
 */
function randomScalar() {
  for (;;) {
    const n = bytesToNumberBE(randomBytes(32));
    if (n > 0n && n < N) return n;
  }
}

/**
 * Build a bLSAG ring signature over `m`.
 *
 * For a Pocket relay the ring is [applicationPubKey, gatewayPubKey] in exactly
 * that order — no sorting, no dedup — and the gateway is the one holding a key,
 * so ourIdx is 1. (The sort-ring-by-bech32-address rule you may have seen
 * applies to the chain-backed path, not to this one.)
 *
 * @param {Uint8Array} m 32-byte signable relay hash
 * @param {Uint8Array[]} ringPubKeys compressed 33-byte SEC1 keys, in ring order
 * @param {Uint8Array} privKey 32-byte big-endian key for ringPubKeys[ourIdx]
 * @param {number} ourIdx index of the signer in ringPubKeys
 * @param {{hashToScalar?: (input: Uint8Array) => bigint}} [opts] test seam, see below
 * @returns {Uint8Array} 69 + 65n bytes (199 for the 2-member relay ring)
 */
export function buildRelaySignature(m, ringPubKeys, privKey, ourIdx, opts = {}) {
  // opts.hashToScalar exists ONLY so the harness can demonstrate that the
  // canonical (right-aligning) derivation really does get rejected by the Go
  // verifier. Production callers must never pass it.
  const h = opts.hashToScalar ?? hashToScalar;

  if (!(m instanceof Uint8Array) || m.length !== 32) {
    throw new Error(
      `m must be a 32-byte Uint8Array (the signable relay hash), got ` +
        `${m?.length ?? 0} bytes of ${m?.constructor?.name ?? typeof m}`,
    );
  }
  if (!Array.isArray(ringPubKeys) || ringPubKeys.length < 2) {
    throw new Error(
      `ringPubKeys must be an array of at least 2 compressed keys, got ` +
        `${Array.isArray(ringPubKeys) ? `${ringPubKeys.length} member(s)` : typeof ringPubKeys}`,
    );
  }
  const size = ringPubKeys.length;
  if (!Number.isInteger(ourIdx) || ourIdx < 0 || ourIdx >= size) {
    throw new Error(`ourIdx ${ourIdx} is out of range for a ${size}-member ring`);
  }
  if (!(privKey instanceof Uint8Array) || privKey.length !== 32) {
    throw new Error(
      `privKey must be a 32-byte big-endian Uint8Array, got ` +
        `${privKey?.length ?? 0} bytes of ${privKey?.constructor?.name ?? typeof privKey}`,
    );
  }

  const x = bytesToNumberBE(privKey);
  if (x <= 0n || x >= N) {
    throw new Error('privKey is not a valid secp256k1 scalar (must be in [1, N))');
  }

  const pubs = ringPubKeys.map((pk, i) => {
    // The 33-byte length is not pedantry, and Point.fromBytes will NOT catch
    // this for you: it happily accepts a 65-byte uncompressed key as the very
    // same point. But hashToCurve hashes these exact input bytes, so an
    // uncompressed key yields a different H(P_i), a different key image, and a
    // signature the verifier rejects 100% of the time — with every other check
    // in this function passing. Enforce the encoding here or debug it later.
    if (!(pk instanceof Uint8Array) || pk.length !== 33) {
      throw new Error(
        `ringPubKeys[${i}] must be a 33-byte compressed SEC1 key, got ` +
          `${pk?.length ?? 0} bytes of ${pk?.constructor?.name ?? typeof pk}` +
          (pk?.length === 65 ? ' (that is an UNCOMPRESSED key; compress it first)' : ''),
      );
    }
    try {
      return Point.fromBytes(pk);
    } catch (err) {
      throw new Error(`ringPubKeys[${i}] is not a valid compressed secp256k1 point: ${err.message}`);
    }
  });

  // Fail loudly on the classic wiring mistake. Signing with a key that is not
  // the ring member at ourIdx yields a signature that is silently rejected
  // 100% of the time, and the verifier cannot tell you why.
  if (!G.multiply(x).equals(pubs[ourIdx])) {
    throw new Error(
      `privKey does not match ringPubKeys[${ourIdx}]; check the ring order ` +
        '(a relay ring is [app, gateway] and the gateway signs, so ourIdx is 1)',
    );
  }

  const c = new Array(size); // challenges
  const s = new Array(size); // responses
  const challenge = (l, r) => h(concatBytes(m, l.toBytes(true), r.toBytes(true)));

  // Our own leg. Commit to a random nonce u, and derive the NEXT member's
  // challenge from it: L = u*G, R = u*H(P_j).
  const hOur = hashToCurve(ringPubKeys[ourIdx]);
  const image = hOur.multiply(x); // key image I = x*H(P_j) — ties double-signs together
  const u = randomScalar();
  c[(ourIdx + 1) % size] = challenge(G.multiply(u), hOur.multiply(u));

  // Walk forward around the ring, forging every decoy leg with a random s. Each
  // leg's challenge feeds the next, so the chain closes back onto ourIdx.
  for (let i = 1; i < size; i++) {
    const idx = (ourIdx + i) % size;
    s[idx] = randomScalar();

    const l = G.multiply(s[idx]).add(pubs[idx].multiply(c[idx]));
    const r = hashToCurve(ringPubKeys[idx]).multiply(s[idx]).add(image.multiply(c[idx]));
    c[(idx + 1) % size] = challenge(l, r);
  }

  // Close the loop. Only someone holding x can produce an s that makes our own
  // leg recompute to the c[ourIdx] the chain just came back around to.
  s[ourIdx] = mod(u - mod(c[ourIdx] * x));

  // Wire format: [4B big-endian n][32B c[0]][33B key image][n x (32B s_i || 33B P_i)]
  const header = new Uint8Array(4);
  new DataView(header.buffer).setUint32(0, size, false); // false => big-endian

  const parts = [header, encodeScalar(c[0]), image.toBytes(true)];
  for (let i = 0; i < size; i++) {
    parts.push(encodeScalar(s[i]), pubs[i].toBytes(true));
  }
  return concatBytes(...parts);
}
