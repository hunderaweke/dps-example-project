// k6 load test for the example endpoints. Run with `make load-test`
// (BASE_URL defaults to the API on the host).
//
// Each virtual user creates an example, then reads it back twice (the second
// read should be a cache hit), then lists one page. Thresholds fail the run on regressions.
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    create_then_read: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '15s', target: 20 },
        { duration: '30s', target: 20 },
        { duration: '10s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:create}': ['p(95)<250'],
    'http_req_duration{name:get}': ['p(95)<100'],
    'http_req_duration{name:list}': ['p(95)<200'],
  },
};

const headers = { 'Content-Type': 'application/json' };

export default function () {
  const payload = JSON.stringify({ name: `load-${__VU}-${__ITER}`, owner_id: `acc_${__VU}` });
  const created = http.post(`${BASE_URL}/v1/examples`, payload, { headers, tags: { name: 'create' } });
  check(created, { 'create is 201': (r) => r.status === 201 });
  if (created.status !== 201) return;

  const id = created.json('id');
  for (let i = 0; i < 2; i++) {
    const got = http.get(`${BASE_URL}/v1/examples/${id}`, { tags: { name: 'get' } });
    check(got, { 'get is 200': (r) => r.status === 200 });
  }

  const listed = http.get(`${BASE_URL}/v1/examples?limit=20`, { tags: { name: 'list' } });
  check(listed, { 'list is 200': (r) => r.status === 200 });
  sleep(0.5);
}
