import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "../index.js";

const SCHEMA = `
node Person {
  name: String @key
  age: I32?
}

node Company {
  name: String @key
}

edge WorksAt: Person -> Company {
  since: I32?
  tags: [String]?
}
`;

const DATA = [
  '{"type": "Person", "data": {"name": "Alice", "age": 30}}',
  '{"type": "Person", "data": {"name": "Bob", "age": 25}}',
  '{"type": "Person", "data": {"name": "Carol", "age": 35}}',
  '{"type": "Company", "data": {"name": "Acme"}}',
  '{"edge": "WorksAt", "from": "Alice", "to": "Acme", "data": {"since": 2020, "tags": ["core"]}}',
  '{"edge": "WorksAt", "from": "Bob", "to": "Acme", "data": {"since": 2022}}',
].join("\n");

const QUERIES = `query allPeople() {
  match {
    $p: Person
  }
  return { $p.name, $p.age }
}

query personByName($name: String) {
  match {
    $p: Person { name: $name }
  }
  return { $p.name, $p.age }
}

query olderThan($minAge: I32) {
  match {
    $p: Person
    $p.age > $minAge
  }
  return { $p.name, $p.age }
  order { $p.age asc }
}

query coworkers() {
  match {
    $p: Person
    $p worksAt $c
    $c: Company
  }
  return { $p.name as person, $c.name as company }
}

query insertPerson($name: String, $age: I32) {
  insert Person { name: $name, age: $age }
}

query deletePerson($name: String) {
  delete Person where name = $name
}

query validateU64($v: U64) {
  match {
    $p: Person
  }
  return { $p.name }
  limit 1
}
`;

let tmpDir;

async function freshDb() {
  const dbPath = join(tmpDir, `test-${Date.now()}.nano`);
  const db = await Database.init(dbPath, SCHEMA);
  await db.load(DATA, "overwrite");
  return { db, dbPath };
}

describe("Database", () => {
  before(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), "nanograph-ts-test-"));
  });

  after(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  describe("init + load", () => {
    it("should init a new database and load data", async () => {
      const { db } = await freshDb();
      const rows = await db.run(QUERIES, "allPeople");
      assert.equal(rows.length, 3);
      const names = rows.map((r) => r.name).sort();
      assert.deepEqual(names, ["Alice", "Bob", "Carol"]);
      db.close();
    });
  });

  describe("run — read queries", () => {
    it("should execute a parameterized query", async () => {
      const { db } = await freshDb();
      const rows = await db.run(QUERIES, "personByName", { name: "Alice" });
      assert.equal(rows.length, 1);
      assert.equal(rows[0].name, "Alice");
      assert.equal(rows[0].age, 30);
      db.close();
    });

    it("should filter with comparison params", async () => {
      const { db } = await freshDb();
      const rows = await db.run(QUERIES, "olderThan", { minAge: 28 });
      assert.equal(rows.length, 2);
      assert.equal(rows[0].name, "Alice");
      assert.equal(rows[1].name, "Carol");
      db.close();
    });

    it("should traverse edges", async () => {
      const { db } = await freshDb();
      const rows = await db.run(QUERIES, "coworkers");
      assert.equal(rows.length, 2);
      for (const row of rows) {
        assert.equal(row.company, "Acme");
      }
      db.close();
    });

    it("should return empty array for no matches", async () => {
      const { db } = await freshDb();
      const rows = await db.run(QUERIES, "personByName", {
        name: "Nobody",
      });
      assert.equal(rows.length, 0);
      db.close();
    });
  });

  describe("run — mutations", () => {
    it("should insert a node", async () => {
      const { db } = await freshDb();
      const result = await db.run(QUERIES, "insertPerson", {
        name: "Dave",
        age: 40,
      });
      assert.equal(result.affectedNodes, 1);

      const rows = await db.run(QUERIES, "personByName", { name: "Dave" });
      assert.equal(rows.length, 1);
      assert.equal(rows[0].age, 40);
      db.close();
    });

    it("should delete a node", async () => {
      const { db } = await freshDb();
      const result = await db.run(QUERIES, "deletePerson", { name: "Carol" });
      assert.equal(result.affectedNodes, 1);

      const rows = await db.run(QUERIES, "allPeople");
      assert.equal(rows.length, 2);
      const names = rows.map((r) => r.name).sort();
      assert.deepEqual(names, ["Alice", "Bob"]);
      db.close();
    });
  });

  describe("check", () => {
    it("should typecheck all queries", async () => {
      const { db } = await freshDb();
      const checks = await db.check(QUERIES);
      assert.ok(Array.isArray(checks));
      assert.ok(checks.length > 0);
      for (const c of checks) {
        assert.equal(c.status, "ok", `query '${c.name}' failed: ${c.error}`);
      }
      db.close();
    });

    it("should report errors for bad queries", async () => {
      const { db } = await freshDb();
      const badQuery = `query broken() {
  match { $x: NonExistentType }
  return { $x.name }
}`;
      const checks = await db.check(badQuery);
      assert.equal(checks.length, 1);
      assert.equal(checks[0].status, "error");
      assert.equal(checks[0].kind, "read");
      assert.ok(checks[0].error.length > 0);
      db.close();
    });
  });

  describe("describe", () => {
    it("should return schema info", async () => {
      const { db } = await freshDb();
      const info = await db.describe();
      assert.ok(Array.isArray(info.nodeTypes));
      assert.ok(Array.isArray(info.edgeTypes));

      const personType = info.nodeTypes.find((t) => t.name === "Person");
      assert.ok(personType, "Person node type should exist");
      assert.ok(personType.properties.length >= 2);

      const worksAtType = info.edgeTypes.find((t) => t.name === "WorksAt");
      assert.ok(worksAtType, "WorksAt edge type should exist");
      assert.equal(worksAtType.srcType, "Person");
      assert.equal(worksAtType.dstType, "Company");
      const tagsProp = worksAtType.properties.find((p) => p.name === "tags");
      assert.ok(tagsProp, "WorksAt.tags should exist");
      assert.equal(tagsProp.list, true);
      db.close();
    });
  });

  describe("doctor", () => {
    it("should report healthy database", async () => {
      const { db } = await freshDb();
      const report = await db.doctor();
      assert.equal(report.healthy, true);
      assert.equal(report.issues.length, 0);
      db.close();
    });
  });

  describe("open", () => {
    it("should reopen an existing database", async () => {
      const { db, dbPath } = await freshDb();
      db.close();

      const db2 = await Database.open(dbPath);
      const rows = await db2.run(QUERIES, "allPeople");
      assert.equal(rows.length, 3);
      db2.close();
    });
  });

  describe("compact + cleanup", () => {
    it("should compact without error", async () => {
      const { db } = await freshDb();
      const result = await db.compact();
      assert.ok(typeof result.datasetsConsidered === "number");
      db.close();
    });

    it("should cleanup without error", async () => {
      const { db } = await freshDb();
      const result = await db.cleanup();
      assert.ok(typeof result.txRowsKept === "number");
      db.close();
    });
  });

  describe("error cases", () => {
    it("should reject bad schema", async () => {
      const dbPath = join(tmpDir, `bad-schema-${Date.now()}.nano`);
      await assert.rejects(
        () => Database.init(dbPath, "this is not valid schema"),
        (err) => {
          assert.ok(err.message.length > 0);
          return true;
        }
      );
    });

    it("should reject missing query name", async () => {
      const { db } = await freshDb();
      await assert.rejects(
        () => db.run(QUERIES, "nonExistentQuery"),
        (err) => {
          assert.ok(err.message.includes("not found"));
          return true;
        }
      );
      db.close();
    });

    it("should reject operations on closed database", async () => {
      const { db } = await freshDb();
      db.close();
      await assert.rejects(
        () => db.run(QUERIES, "allPeople"),
        (err) => {
          assert.ok(err.message.includes("closed"));
          return true;
        }
      );
    });

    it("should reject unsafe U64 number params", async () => {
      const { db } = await freshDb();
      await assert.rejects(
        () => db.run(QUERIES, "validateU64", { v: Number.MAX_SAFE_INTEGER + 1 }),
        (err) => {
          assert.ok(err.message.includes("decimal string"));
          return true;
        }
      );
      db.close();
    });

    it("should accept U64 decimal string params for exact values", async () => {
      const { db } = await freshDb();
      const rows = await db.run(QUERIES, "validateU64", {
        v: "9007199254740993",
      });
      assert.equal(rows.length, 1);
      assert.equal(rows[0].name, "Alice");
      db.close();
    });
  });
});
