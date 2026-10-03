#!/usr/bin/env node
/**
 * Test harness: sign with sign.mjs, verify with the real Go verifier.
 *
 * Round-tripping your own sign -> your own verify proves nothing. An
 * implementation can be perfectly self-consistent and still be rejected by the
 * relayer for its entire life. So every signature here is handed to the oracle,
 * which runs the same github.com/pokt-network/ring-go the relayer runs.
 *
 * WHY 1500 SIGNATURES, AND WHY YOU MUST NOT LOWER IT
 * --------------------------------------------------
 * The left-align quirk in hashToScalar fires on ~1 hash in 256. A 2-member ring
 * derives 2 challenges per signature, so an implementation that gets the quirk
 * wrong still produces a VALID signature ~99.2% of the time. At 10 signatures a
 * broken port passes with probability 0.92; at 50, 0.68. It looks finished, it
 * ships, and it silently loses ~1 relay in 128 forever.
 *
 * 1500 signatures puts P(a broken port passing) at ~1e-5. That is the whole
 * reason this file exists rather than a quick 10-message smoke test.
 *
 * Step 4 is the negative control: it deliberately signs with the canonical
 * (right-aligning) derivation and requires the oracle to REJECT some. Without
 * it, a harness that had quietly stopped exercising the quirk — wrong oracle,
 * vectors regenerated, signatures not actually reaching the verifier — would
 * report a green 1500/1500 and mean nothing.
 *
 * Usage:
 *   go build -o ./oracle-bin ../oracle && ORACLE=./oracle-bin node verify-against-oracle.mjs
 *
 * Takes ~1 minute: 3000 signatures, each verified in its own oracle process.
 */

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import { sha3_256, sha3_512 } from '@noble/hashes/sha3.js';
import { secp256k1 } from '@noble/curves/secp256k1.js';
import { bytesToHex, bytesToNumberBE, hexToBytes, randomBytes } from '@noble/curves/utils.js';

import { buildRelaySignature, encodeScalar, hashToCurve, hashToScalar } from './sign.mjs';

const N = secp256k1.Point.Fn.ORDER;
const TRIALS = 1500;

const here = dirname(fileURLToPath(import.meta.url));
const ORACLE = process.env.ORACLE ?? resolve(here, 'oracle-bin');

let failures = 0;
const fail = (msg) => {
  failures++;
  console.error(`  FAIL  ${msg}`);
};
const pass = (msg) => console.log(`  ok    ${msg}`);

/** Run the oracle. Returns {status, stdout}; a non-zero status is data, not an error. */
function oracle(args, input) {
  const res = spawnSync(ORACLE, args, { input, encoding: 'utf8' });
  if (res.error?.code === 'ENOENT') {
    console.error(
      `\nCannot find the oracle at ${ORACLE}\n\n` +
        'The oracle is the authority this harness tests against. Build it:\n\n' +
        `  go build -o ${ORACLE} ${resolve(here, '../oracle')}\n\n` +
        'or point ORACLE=/path/to/oracle at an existing build.\n',
    );
    process.exit(2);
  }
  if (res.error) throw res.error;
  return res;
}

// ---------------------------------------------------------------------------
// Step 1: SHA-3 is FIPS-202, not Keccak.
// ---------------------------------------------------------------------------
// Both are exported by @noble/hashes under confusingly adjacent names. Picking
// the wrong one produces a well-formed signature that is always rejected, with
// no diagnostic anywhere. These are the NIST FIPS-202 known-answer values for
// the empty input, so this pins the choice independently of the oracle.
console.log('\n[1/4] SHA-3 is FIPS-202, not Keccak');
{
  const KAT = {
    sha3_512:
      'a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a6' +
      '15b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26',
    sha3_256: 'a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a',
  };
  const got512 = bytesToHex(sha3_512(new Uint8Array(0)));
  const got256 = bytesToHex(sha3_256(new Uint8Array(0)));

  got512 === KAT.sha3_512
    ? pass('sha3_512("") matches the NIST FIPS-202 known-answer test')
    : fail(`sha3_512("") = ${got512}\n        want NIST FIPS-202 ${KAT.sha3_512}\n        (if this is 0eab42de... you imported keccak_512)`);

  got256 === KAT.sha3_256
    ? pass('sha3_256("") matches the NIST FIPS-202 known-answer test')
    : fail(`sha3_256("") = ${got256}\n        want NIST FIPS-202 ${KAT.sha3_256}\n        (if this is c5d24601... you imported keccak_256)`);
}

// ---------------------------------------------------------------------------
// Step 2: the deterministic vectors.
// ---------------------------------------------------------------------------
// A signature is randomized and cannot be diffed, so when step 3 fails these
// are what tell you WHICH primitive drifted.
console.log('\n[2/4] Deterministic vectors from `oracle vectors`');
const vectors = JSON.parse(oracle(['vectors']).stdout);
{
  // Guard the guard: the SHORT-reduction probe is the only vector that can
  // catch a canonical hashToScalar. If it ever disappears from the oracle's
  // output, every port silently loses its one deterministic check on the quirk.
  const short = vectors.hash_to_scalar.filter((v) => v.note?.includes('SHORT'));
  short.length > 0
    ? pass(`vector file still pins the quirk (${short.length} SHORT-reduction probe)`)
    : fail('no SHORT-reduction vector in `oracle vectors` — the quirk is no longer pinned deterministically');

  for (const v of vectors.hash_to_scalar) {
    const got = bytesToHex(encodeScalar(hashToScalar(hexToBytes(v.input_hex))));
    const isShort = v.note?.includes('SHORT');
    got === v.output_hex
      ? pass(`hash_to_scalar ${v.input_hex}${isShort ? '  <- the SHORT reduction; a canonical port fails HERE' : ''}`)
      : fail(`hash_to_scalar(${v.input_hex})\n        got  ${got}\n        want ${v.output_hex}`);
  }

  for (const v of vectors.hash_to_curve) {
    const got = bytesToHex(hashToCurve(hexToBytes(v.input_hex)).toBytes(true));
    got === v.output_hex
      ? pass(`hash_to_curve  ${v.input_hex.slice(0, 18)}...`)
      : fail(`hash_to_curve(${v.input_hex})\n        got  ${got}\n        want ${v.output_hex}`);
  }

  for (const v of vectors.scalar_encoding) {
    const got = bytesToHex(encodeScalar(bytesToNumberBE(hexToBytes(v.input_hex))));
    got === v.output_hex
      ? pass('scalar_encoding is right-aligned on the wire (the opposite of hash_to_scalar)')
      : fail(`encodeScalar(0x${v.input_hex})\n        got  ${got}\n        want ${v.output_hex}`);
  }
}

// The relay ring: [app, gateway], in that order, gateway signs.
const appPub = hexToBytes(vectors.keys.app_pub_hex);
const gwPub = hexToBytes(vectors.keys.gateway_pub_hex);
const gwPriv = hexToBytes(vectors.keys.gateway_priv_hex);
const ring = [appPub, gwPub];
const OUR_IDX = 1;

/** Sign, hand the bytes to the Go verifier, report what it says. */
function oracleAccepts(m, sig) {
  const res = oracle(['verify'], JSON.stringify({ msg_hex: bytesToHex(m), sig_hex: bytesToHex(sig) }));
  return { accepted: res.status === 0, reason: JSON.parse(res.stdout || '{}').reason };
}

// ---------------------------------------------------------------------------
// Step 3: sign 1500 distinct messages; the Go verifier must accept every one.
// ---------------------------------------------------------------------------
console.log(`\n[3/4] ${TRIALS} distinct messages -> ring-go must accept 100%`);
{
  const messages = Array.from({ length: TRIALS }, () => randomBytes(32));
  const distinct = new Set(messages.map(bytesToHex)).size;
  distinct === TRIALS
    ? pass(`${TRIALS} messages, all distinct`)
    : fail(`only ${distinct}/${TRIALS} messages were distinct`);

  const rejected = [];
  let badLength = 0;
  const t0 = Date.now();

  for (const [i, m] of messages.entries()) {
    const sig = buildRelaySignature(m, ring, gwPriv, OUR_IDX);
    if (sig.length !== 199) badLength++;

    const { accepted, reason } = oracleAccepts(m, sig);
    if (!accepted) rejected.push({ i, msg: bytesToHex(m), sig: bytesToHex(sig), reason });

    if ((i + 1) % 250 === 0) {
      process.stdout.write(`        ${i + 1}/${TRIALS} signed and verified, ${rejected.length} rejected\n`);
    }
  }
  const secs = ((Date.now() - t0) / 1000).toFixed(1);

  badLength === 0
    ? pass('every signature is 199 bytes (69 + 65*2)')
    : fail(`${badLength} signatures had the wrong length`);

  if (rejected.length === 0) {
    pass(`ring-go accepted ${TRIALS}/${TRIALS} (100%) in ${secs}s`);
  } else {
    const rate = ((rejected.length / TRIALS) * 100).toFixed(2);
    fail(
      `ring-go rejected ${rejected.length}/${TRIALS} (${rate}%). ` +
        `A rate near 0.78% means hashToScalar is not reproducing the left-align quirk ` +
        `(see sign.mjs). Retrying will NOT help — the quirk is deterministic per input.\n` +
        `        first rejection: ${rejected[0].reason}\n` +
        `        msg ${rejected[0].msg}\n        sig ${rejected[0].sig}`,
    );
  }
}

// ---------------------------------------------------------------------------
// Step 4: negative control — prove this harness can actually detect the bug.
// ---------------------------------------------------------------------------
// Sign with the canonical (right-aligning) derivation, i.e. the implementation
// a careful person writes by instinct, and require the oracle to reject some.
// If this passes 1500/1500 then step 3's green is meaningless.
console.log(`\n[4/4] Negative control: ${TRIALS} signatures with a CANONICAL hashToScalar`);
{
  // The natural, wrong implementation: reduce mod N and stop. No left-align.
  const canonicalHashToScalar = (input) => bytesToNumberBE(sha3_512(input)) % N;

  let rejected = 0;
  for (let i = 0; i < TRIALS; i++) {
    const m = randomBytes(32);
    const sig = buildRelaySignature(m, ring, gwPriv, OUR_IDX, { hashToScalar: canonicalHashToScalar });
    if (!oracleAccepts(m, sig).accepted) rejected++;
  }

  const rate = (rejected / TRIALS) * 100;
  if (rejected === 0) {
    fail(
      `a canonical hashToScalar was accepted ${TRIALS}/${TRIALS} times. Expected ~0.78% ` +
        'rejections. Either this harness is not really reaching the Go verifier, or ' +
        'go-dleq dropped the left-align — in which case sign.mjs and its comments must ' +
        'change together, and every existing port breaks.',
    );
  } else if (rate > 5) {
    fail(
      `canonical rejection rate ${rate.toFixed(2)}% (${rejected}/${TRIALS}), expected ~0.78%. ` +
        'A rate this high means something broader than the left-align is wrong.',
    );
  } else {
    pass(
      `ring-go rejected ${rejected}/${TRIALS} = ${rate.toFixed(2)}% of canonical signatures ` +
        `(predicted ~0.78%) — the quirk is real, and step 3 is a meaningful check`,
    );
  }
}

// ---------------------------------------------------------------------------
console.log(
  failures === 0
    ? `\nPASS — ${TRIALS} signatures accepted by ring-go, 100%. The quirk is replicated.\n`
    : `\nFAIL — ${failures} check(s) failed.\n`,
);
process.exit(failures === 0 ? 0 : 1);
