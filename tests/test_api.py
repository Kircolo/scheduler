import unittest
import json

from fastapi.testclient import TestClient

from server import app


class TestSchedulerAPIExtended(unittest.TestCase):
    def setUp(self):
        # Entering the context triggers lifespan startup:
        # app.state.scheduler and app.state.lock get created here.
        self.client = TestClient(app)
        self.client.__enter__()

    def tearDown(self):
        # Exiting triggers lifespan shutdown (and resets cleanly per test).
        self.client.__exit__(None, None, None)

    def _job(self, **overrides):
        base = {
            "job_id": "j",
            "tenant_id": "t1",
            "gpu_type": "A100",
            "gpus": 1,
            "priority": 5,
            "submitted_at": 0,
        }
        base.update(overrides)
        return base

    # --------------------
    # Negative-path tests (API)
    # --------------------

    def test_get_unknown_job_is_404_and_has_error_shape(self):
        r = self.client.get("/jobs/does-not-exist")
        self.assertEqual(r.status_code, 404)
        body = r.json()
        self.assertIn("error", body)
        self.assertEqual(body["error"]["code"], "JOB_NOT_FOUND")

    def test_complete_unknown_job_is_404(self):
        r = self.client.post("/jobs/nope/complete")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.json()["error"]["code"], "JOB_NOT_FOUND")

    def test_complete_waiting_job_is_400(self):
        self.client.post("/jobs", json=self._job(job_id="w1"))
        r = self.client.post("/jobs/w1/complete")
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["error"]["code"], "INVALID_STATE")

    def test_schedule_requires_now_is_422(self):
        r = self.client.post("/schedule")
        self.assertEqual(r.status_code, 422)
        body = r.json()
        self.assertEqual(body["error"]["code"], "VALIDATION_ERROR")

    def test_schedule_now_must_be_int_is_422(self):
        r = self.client.post("/schedule", params={"now": "abc"})
        self.assertEqual(r.status_code, 422)
        body = r.json()
        self.assertEqual(body["error"]["code"], "VALIDATION_ERROR")

    def test_submit_missing_field_is_422(self):
        bad = self._job()
        bad.pop("job_id")
        r = self.client.post("/jobs", json=bad)
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["error"]["code"], "VALIDATION_ERROR")

    def test_duplicate_job_id_returns_409_with_code(self):
        job = self._job(job_id="dup")
        r1 = self.client.post("/jobs", json=job)
        self.assertEqual(r1.status_code, 201)

        r2 = self.client.post("/jobs", json=job)
        self.assertEqual(r2.status_code, 409)
        body = r2.json()
        self.assertEqual(body["error"]["code"], "DUPLICATE_JOB")

    # --------------------
    # /stats tests (server.py -> scheduler.stats())
    # --------------------

    def test_stats_initial_state(self):
        r = self.client.get("/stats")
        self.assertEqual(r.status_code, 200)
        st = r.json()

        self.assertEqual(st["total_jobs"], 0)
        self.assertEqual(st["jobs"], {"WAITING": 0, "RUNNING": 0, "COMPLETED": 0})
        self.assertEqual(st["running"], [])
        self.assertEqual(st["waiting"], 0)

        self.assertIn("capacity", st)
        self.assertIn("tenant_quotas", st)

    def test_stats_after_schedule_and_complete(self):
        # Two jobs that fully consume A100 global capacity (4), via 2+2.
        self.client.post("/jobs", json=self._job(job_id="j1", tenant_id="t1", gpus=2))
        self.client.post("/jobs", json=self._job(job_id="j2", tenant_id="t2", gpus=2))

        r = self.client.post("/schedule", params={"now": 0})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(set(r.json()["started"]), {"j1", "j2"})
        self.assertEqual(r.json()["waiting"], 0)

        st = self.client.get("/stats").json()
        self.assertEqual(st["jobs"]["RUNNING"], 2)
        self.assertEqual(st["capacity"]["used"]["A100"], 4)
        self.assertEqual(st["capacity"]["available"]["A100"], 0)

        # Complete one and verify usage drops.
        r = self.client.post("/jobs/j1/complete")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "COMPLETED")

        st2 = self.client.get("/stats").json()
        self.assertEqual(st2["jobs"]["COMPLETED"], 1)
        self.assertEqual(st2["jobs"]["RUNNING"], 1)
        self.assertEqual(st2["capacity"]["used"]["A100"], 2)
        self.assertEqual(st2["capacity"]["available"]["A100"], 2)
        self.assertEqual(st2["running"], ["j2"])

    # --------------------
    # /jobs/batch tests
    # --------------------

    def test_submit_jobs_batch_partial_success(self):
        batch = [
            self._job(job_id="b1", tenant_id="t1", gpus=1),
            {"tenant_id": "t1", "gpu_type": "A100", "gpus": 1, "priority": 1, "submitted_at": 0},  # missing job_id
            self._job(job_id="b1", tenant_id="t1", gpus=1),  # duplicate (same as first)
            self._job(job_id="b2", tenant_id="t2", gpus=0),  # invalid gpus
        ]

        r = self.client.post("/jobs/batch", json=batch)
        self.assertEqual(r.status_code, 200)
        body = r.json()

        self.assertEqual(body["counts"]["accepted"], 1)
        self.assertEqual(body["counts"]["rejected"], 3)
        self.assertEqual(body["accepted"], ["b1"])

        # Rejected rows should include per-item codes.
        codes = [rej["code"] for rej in body["rejected"]]
        self.assertIn("VALIDATION_ERROR", codes)
        self.assertIn("DUPLICATE_JOB", codes)

    # --------------------
    # /ingest tests
    # --------------------

    def test_ingest_ndjson_partial_success(self):
        # Mix of:
        # - a good job
        # - invalid JSON
        # - JSON that's not an object
        # - a duplicate of the good job
        ndjson_lines = [
            json.dumps(self._job(job_id="n1")),
            "{ this is not json }",
            "[]",
            json.dumps(self._job(job_id="n1")),
        ]
        content = "\n".join(ndjson_lines) + "\n"

        r = self.client.post(
            "/ingest",
            params={"fmt": "ndjson"},
            files={"file": ("jobs.ndjson", content.encode("utf-8"), "application/octet-stream")},
        )
        self.assertEqual(r.status_code, 200)
        body = r.json()

        self.assertEqual(body["counts"]["accepted"], 1)
        self.assertEqual(body["accepted"], ["n1"])
        self.assertEqual(body["counts"]["rejected"], 3)

        codes = [rej["code"] for rej in body["rejected"]]
        self.assertIn("PARSE_ERROR", codes)
        self.assertIn("DUPLICATE_JOB", codes)

    def test_ingest_csv_partial_success(self):
        csv_text = (
            "job_id,tenant_id,gpu_type,gpus,priority,submitted_at\n"
            "c1,t1,A100,1,5,0\n"
            "c1,t1,A100,1,5,0\n"   # duplicate
            "c2,t1,A100,0,5,0\n"   # invalid gpus
        )
        r = self.client.post(
            "/ingest",
            params={"fmt": "csv"},
            files={"file": ("jobs.csv", csv_text.encode("utf-8"), "text/csv")},
        )
        self.assertEqual(r.status_code, 200)
        body = r.json()

        self.assertEqual(body["counts"]["accepted"], 1)
        self.assertEqual(body["accepted"], ["c1"])
        self.assertEqual(body["counts"]["rejected"], 2)

        codes = [rej["code"] for rej in body["rejected"]]
        self.assertIn("DUPLICATE_JOB", codes)
        self.assertIn("VALIDATION_ERROR", codes)

    def test_ingest_bad_format_is_400(self):
        r = self.client.post(
            "/ingest",
            params={"fmt": "parquet"},
            files={"file": ("x.bin", b"{}", "application/octet-stream")},
        )
        self.assertEqual(r.status_code, 400)
        body = r.json()
        self.assertEqual(body["error"]["code"], "BAD_FORMAT")

    def test_ingest_bad_encoding_is_400(self):
        # invalid UTF-8 bytes
        r = self.client.post(
            "/ingest",
            params={"fmt": "ndjson"},
            files={"file": ("bad.bin", b"\xff\xfe\xfa", "application/octet-stream")},
        )
        self.assertEqual(r.status_code, 400)
        body = r.json()
        self.assertEqual(body["error"]["code"], "BAD_ENCODING")


if __name__ == "__main__":
    unittest.main()
