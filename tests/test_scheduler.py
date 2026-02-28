import unittest

from scheduler import Job, Scheduler, AGING_SECONDS


class TestScheduler(unittest.TestCase):
    def make_scheduler(self) -> Scheduler:
        capacity = {"A100": 4, "H100": 2}
        quotas = {
            "t1": {"A100": 2, "H100": 1},
            "t2": {"A100": 2, "H100": 1},
        }
        return Scheduler(capacity=capacity, tenant_quotas=quotas)

    def test_submit_and_status_waiting(self):
        s = self.make_scheduler()
        s.submit(Job("j1", "t1", "A100", 1, 5, 0))
        self.assertEqual(s.status("j1"), "WAITING")
        self.assertEqual(s.waiting(), 1)

    def test_submit_duplicate_raises(self):
        s = self.make_scheduler()
        s.submit(Job("j1", "t1", "A100", 1, 1, 0))
        with self.assertRaises(ValueError):
            s.submit(Job("j1", "t1", "A100", 1, 1, 0))

    def test_schedule_respects_capacity(self):
        s = self.make_scheduler()
        # capacity A100=4, each job needs 2
        s.submit(Job("j1", "t1", "A100", 2, 1, 0))
        s.submit(Job("j2", "t2", "A100", 2, 1, 0))
        s.submit(Job("j3", "t1", "A100", 2, 1, 0))
        started = s.schedule(now=0)
        self.assertEqual(set(started), {"j1", "j2"})
        self.assertEqual(s.status("j3"), "WAITING")

    def test_schedule_respects_tenant_quota(self):
        s = self.make_scheduler()
        # t1 quota A100=2
        s.submit(Job("j1", "t1", "A100", 2, 10, 0))
        s.submit(Job("j2", "t1", "A100", 1, 9, 0))
        started = s.schedule(now=0)
        self.assertEqual(started, ["j1"])
        self.assertEqual(s.status("j2"), "WAITING")

    def test_priority_ordering(self):
        s = self.make_scheduler()
        s.submit(Job("j_low", "t1", "H100", 1, 1, 0))
        s.submit(Job("j_high", "t2", "H100", 1, 10, 0))
        started = s.schedule(now=0)
        self.assertEqual(started, ["j_high", "j_low"])

    def test_tie_breakers_submitted_at_then_job_id(self):
        s = self.make_scheduler()
        # same effective priority, earlier submitted_at wins
        s.submit(Job("b", "t1", "A100", 1, 5, 10))
        s.submit(Job("a", "t2", "A100", 1, 5, 0))
        started = s.schedule(now=10)
        self.assertEqual(started[0], "a")

        # exact tie on submitted_at, job_id decides
        s2 = self.make_scheduler()
        s2.submit(Job("b", "t1", "A100", 1, 5, 0))
        s2.submit(Job("a", "t2", "A100", 1, 5, 0))
        started2 = s2.schedule(now=0)
        self.assertEqual(started2[0], "a")

    def test_aging_allows_starved_job_to_overtake(self):
        s = Scheduler(
        capacity={"A100": 1},
        tenant_quotas={"t1": {"A100": 1}, "t2": {"A100": 1}},
    )
        # low priority but submitted long ago
        old = Job("old", "t1", "A100", 1, 1, 0)
        new = Job("new", "t2", "A100", 1, 3, 10)

        s.submit(old)
        s.submit(new)

        # At now=10, effective(old)=1, effective(new)=3 -> new first
        started = s.schedule(now=10)
        self.assertEqual(started[0], "new")
        s.complete("new")

        # Advance time so old gains enough aging
        now = 10 + 3 * AGING_SECONDS
        # submit another high-ish job at this moment
        s.submit(Job("fresh", "t2", "A100", 1, 3, now))
        started2 = s.schedule(now=now)
        # old has effective 1 + 3 = 4, should beat fresh (3)
        self.assertEqual(started2[0], "old")

    def test_complete_frees_resources(self):
        s = self.make_scheduler()
        s.submit(Job("j1", "t1", "H100", 1, 5, 0))
        s.submit(Job("j2", "t2", "H100", 1, 4, 0))
        started = s.schedule(now=0)
        self.assertEqual(started, ["j1", "j2"])

        # H100 fully used now; third job must wait
        s.submit(Job("j3", "t1", "H100", 1, 100, 0))
        started2 = s.schedule(now=0)
        self.assertEqual(started2, [])
        self.assertEqual(s.status("j3"), "WAITING")

        s.complete("j1")
        started3 = s.schedule(now=1)
        self.assertEqual(started3, ["j3"])
        self.assertEqual(s.status("j3"), "RUNNING")

    def test_complete_unknown_raises_keyerror(self):
        s = self.make_scheduler()
        with self.assertRaises(KeyError):
            s.complete("missing")

    def test_complete_non_running_raises_valueerror(self):
        s = self.make_scheduler()
        s.submit(Job("j1", "t1", "A100", 1, 1, 0))
        with self.assertRaises(ValueError):
            s.complete("j1")

    def test_status_unknown_raises_keyerror(self):
        s = self.make_scheduler()
        with self.assertRaises(KeyError):
            s.status("nope")

    def test_running_sorted(self):
        s = self.make_scheduler()
        s.submit(Job("j2", "t2", "A100", 1, 1, 0))
        s.submit(Job("j1", "t1", "A100", 1, 1, 0))
        started = s.schedule(now=5)
        self.assertEqual(set(started), {"j1", "j2"})
        self.assertEqual(s.running(), ["j1", "j2"])

    def test_stats_empty(self):
        s = self.make_scheduler()
        st = s.stats()

        self.assertEqual(st["total_jobs"], 0)
        self.assertEqual(st["jobs"], {"WAITING": 0, "RUNNING": 0, "COMPLETED": 0})
        self.assertEqual(st["running"], [])
        self.assertEqual(st["waiting"], 0)

        self.assertEqual(st["capacity"]["total"]["A100"], 4)
        self.assertEqual(st["capacity"]["used"]["A100"], 0)
        self.assertEqual(st["capacity"]["available"]["A100"], 4)

        self.assertEqual(st["tenant_quotas"]["t1"]["total"]["A100"], 2)
        self.assertEqual(st["tenant_quotas"]["t1"]["used"]["A100"], 0)
        self.assertEqual(st["tenant_quotas"]["t1"]["available"]["A100"], 2)

    def test_stats_after_run_and_complete(self):
        s = self.make_scheduler()
        s.submit(Job("j1", "t1", "A100", 2, 1, 0))
        s.submit(Job("j2", "t2", "A100", 2, 1, 0))

        started = s.schedule(now=0)
        self.assertEqual(started, ["j1", "j2"])

        st = s.stats()
        self.assertEqual(st["jobs"]["RUNNING"], 2)
        self.assertEqual(st["jobs"]["WAITING"], 0)
        self.assertEqual(st["jobs"]["COMPLETED"], 0)
        self.assertEqual(st["capacity"]["used"]["A100"], 4)
        self.assertEqual(st["capacity"]["available"]["A100"], 0)
        self.assertEqual(st["tenant_quotas"]["t1"]["used"]["A100"], 2)
        self.assertEqual(st["tenant_quotas"]["t2"]["used"]["A100"], 2)

        s.complete("j1")
        st2 = s.stats()
        self.assertEqual(st2["jobs"]["COMPLETED"], 1)
        self.assertEqual(st2["jobs"]["RUNNING"], 1)
        self.assertEqual(st2["capacity"]["used"]["A100"], 2)
        self.assertEqual(st2["capacity"]["available"]["A100"], 2)
        self.assertEqual(st2["tenant_quotas"]["t1"]["used"]["A100"], 0)
        self.assertEqual(st2["tenant_quotas"]["t1"]["available"]["A100"], 2)
        self.assertEqual(st2["running"], ["j2"])

    def test_schedule_skips_unknown_tenant_or_gpu_type(self):
        s = self.make_scheduler()
        s.submit(Job("bad_tenant", "t999", "A100", 1, 100, 0))
        s.submit(Job("bad_gpu", "t1", "V100", 1, 100, 0))

        started = s.schedule(now=0)
        self.assertEqual(started, [])
        self.assertEqual(s.status("bad_tenant"), "WAITING")
        self.assertEqual(s.status("bad_gpu"), "WAITING")

    def test_scenario_submit_schedule_complete_schedule(self):
        """Scenario test:
        Submit 5 jobs across tenants/types -> schedule tick -> complete -> schedule again
        -> assert state transitions.
        """
        s = self.make_scheduler()

        # 3x A100 jobs
        s.submit(Job("jA1", "t1", "A100", 2, 10, 0))  # should start
        s.submit(Job("jA2", "t2", "A100", 2, 9, 0))   # should start
        s.submit(Job("jA3", "t1", "A100", 1, 8, 0))   # blocked by t1 A100 quota until jA1 completes

        # 2x H100 jobs
        s.submit(Job("jH1", "t1", "H100", 1, 7, 0))   # should start
        s.submit(Job("jH2", "t2", "H100", 1, 6, 0))   # should start

        started1 = s.schedule(now=0)
        self.assertEqual(set(started1), {"jA1", "jA2", "jH1", "jH2"})
        self.assertEqual(s.status("jA3"), "WAITING")

        # Complete one A100 job and schedule again; jA3 should now fit.
        s.complete("jA1")
        started2 = s.schedule(now=1)
        self.assertEqual(started2, ["jA3"])

        self.assertEqual(s.status("jA1"), "COMPLETED")
        self.assertEqual(s.status("jA2"), "RUNNING")
        self.assertEqual(s.status("jA3"), "RUNNING")
        self.assertEqual(s.status("jH1"), "RUNNING")
        self.assertEqual(s.status("jH2"), "RUNNING")

        # Running order is by started_at then job_id (so jobs from tick 0 first).
        self.assertEqual(s.running(), ["jA2", "jH1", "jH2", "jA3"])

        st = s.stats()
        self.assertEqual(st["jobs"]["WAITING"], 0)
        self.assertEqual(st["jobs"]["COMPLETED"], 1)
        self.assertEqual(st["jobs"]["RUNNING"], 4)
        self.assertEqual(st["total_jobs"], 5)


if __name__ == "__main__":
    unittest.main()
