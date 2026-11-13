// Tadka Lesson 01: a fixed-rate traffic spike on POST /orders.
//
// constant-arrival-rate keeps the INCOMING rate fixed even when the API slows down.
// A plain virtual-user loop would slow itself down and hide the waiting.
//
// Usage:
//   k6 run -e RATE=20  -e DURATION=10s load-tests/order-spike.js
//   k6 run -e RATE=40  -e DURATION=10s load-tests/order-spike.js
//   k6 run -e RATE=100 -e DURATION=30s load-tests/order-spike.js
//
// Read the real output. Nothing here predicts a result.

import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:5080';
const RATE = parseInt(__ENV.RATE || '20', 10);
const DURATION = __ENV.DURATION || '10s';

export const options = {
  scenarios: {
    spike: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      // Enough VUs that k6 itself never becomes the limit. Requests can wait up to the
      // pool timeout (15 s), so at 100/s well over a thousand can be open at once.
      preAllocatedVUs: parseInt(__ENV.PRE_VUS || '2000', 10),
      maxVUs: parseInt(__ENV.MAX_VUS || '3000', 10),
    },
  },
  summaryTrendStats: ['min', 'avg', 'med', 'max'],
};

const body = JSON.stringify({ customerId: 42, restaurantId: 7, totalAmount: 349.0 });
// Client timeout (60 s) is longer than the pool timeout (15 s), so a failure comes from
// the server's pool and not from k6 giving up first.
const params = { headers: { 'Content-Type': 'application/json' }, timeout: '60s' };

export default function () {
  const res = http.post(`${BASE_URL}/orders`, body, params);
  check(res, {
    'status is 201': (r) => r.status === 201,
  });
}
