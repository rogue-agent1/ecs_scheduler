#!/usr/bin/env python3
"""ecs_scheduler — Cron-like task scheduler with expression parsing. Zero deps."""
import time

class CronExpr:
    def __init__(self, expr):
        parts = expr.split()
        assert len(parts) == 5, "Need: min hour dom month dow"
        self.fields = [self._parse_field(p, r) for p, r in
                       zip(parts, [(0,59),(0,23),(1,31),(1,12),(0,6)])]

    def _parse_field(self, field, rng):
        lo, hi = rng
        if field == '*': return set(range(lo, hi+1))
        result = set()
        for part in field.split(','):
            if '/' in part:
                base, step = part.split('/')
                start = lo if base == '*' else int(base)
                result.update(range(start, hi+1, int(step)))
            elif '-' in part:
                a, b = part.split('-')
                result.update(range(int(a), int(b)+1))
            else:
                result.add(int(part))
        return result

    def matches(self, t=None):
        if t is None: t = time.localtime()
        return (t.tm_min in self.fields[0] and t.tm_hour in self.fields[1] and
                t.tm_mday in self.fields[2] and t.tm_mon in self.fields[3] and
                t.tm_wday in self.fields[4])

    def next_run(self, after=None):
        if after is None: after = time.time()
        t = after + 60 - (after % 60)
        for _ in range(525960):  # max 1 year
            lt = time.localtime(t)
            if self.matches(lt): return t
            t += 60
        return None

class Scheduler:
    def __init__(self):
        self.jobs = []

    def add(self, name, cron_expr, fn):
        self.jobs.append({'name': name, 'cron': CronExpr(cron_expr), 'fn': fn, 'expr': cron_expr})

    def check(self, t=None):
        due = []
        for job in self.jobs:
            if job['cron'].matches(t):
                due.append(job)
        return due

    def describe(self):
        now = time.time()
        for job in self.jobs:
            nxt = job['cron'].next_run(now)
            nxt_str = time.strftime('%Y-%m-%d %H:%M', time.localtime(nxt)) if nxt else 'never'
            print(f"  {job['name']:.<30} {job['expr']:.<20} next: {nxt_str}")

def main():
    s = Scheduler()
    s.add("backup", "0 2 * * *", lambda: print("backing up..."))
    s.add("health_check", "*/5 * * * *", lambda: print("checking..."))
    s.add("weekly_report", "0 9 * * 1", lambda: print("reporting..."))
    s.add("midnight", "0 0 1 * *", lambda: print("monthly..."))

    print("Cron Scheduler:\n")
    s.describe()

    # Test matching
    print("\nExpression tests:")
    tests = [
        ("*/15 * * * *", "every 15 min"),
        ("0 9-17 * * 1-5", "work hours weekdays"),
        ("30 4 1,15 * *", "4:30 AM on 1st and 15th"),
    ]
    for expr, desc in tests:
        c = CronExpr(expr)
        nxt = c.next_run()
        print(f"  {expr:.<25} {desc:.<30} next: {time.strftime('%m/%d %H:%M', time.localtime(nxt))}")

if __name__ == "__main__":
    main()
