import Foundation
import NanoGraph
import XCTest

final class DatabaseTests: XCTestCase {
    private struct CheckRow: Decodable {
        let name: String
        let kind: String
        let status: String
        let error: String?
    }

    private struct DescribeResult: Decodable {
        struct TypeDef: Decodable {
            let name: String
        }

        let nodeTypes: [TypeDef]
        let edgeTypes: [TypeDef]
    }

    private let schema = """
    node Person {
      name: String @key
      age: I32?
      role: enum(engineer, manager)?
      active: Bool?
    }
    """

    private let data = [
        #"{"type":"Person","data":{"name":"Alice","age":30,"role":"engineer","active":true}}"#,
        #"{"type":"Person","data":{"name":"Bob","age":25,"role":"manager","active":false}}"#,
    ].joined(separator: "\n")

    private let queries = """
    query allPeople() {
      match { $p: Person }
      return { $p.name as name, $p.age as age }
      order { $p.name asc }
    }

    query byRole($role: String) {
      match { $p: Person { role: $role } }
      return { $p.name as name }
    }
    """

    func testInitLoadRunAndTypedDecode() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        let raw = try db.run(querySource: queries, queryName: "allPeople")
        let rows = try castRows(raw)
        XCTAssertEqual(rows.count, 2)
        XCTAssertEqual(rows.compactMap { $0["name"] as? String }, ["Alice", "Bob"])

        struct PersonRow: Decodable {
            let name: String
            let age: Int?
        }
        let typedRows = try db.run([PersonRow].self, querySource: queries, queryName: "allPeople")
        XCTAssertEqual(typedRows.map(\.name), ["Alice", "Bob"])
        XCTAssertEqual(typedRows.map(\.age), [30, 25])
    }

    func testErrorDescriptionExposesMessage() {
        let error = NanoGraphError.message("boom")
        XCTAssertEqual(error.errorDescription, "boom")
    }

    func testCloseIsIdempotentAndBlocksFurtherCalls() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }

        try db.close()
        XCTAssertNoThrow(try db.close())
        XCTAssertThrowsError(try db.describe()) { error in
            XCTAssertTrue(errorMessage(error).contains("Database is closed"))
        }
    }

    func testRunRejectsNonStringForStringParam() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        XCTAssertThrowsError(
            try db.run(querySource: queries, queryName: "byRole", params: ["role": 42])
        ) { error in
            XCTAssertTrue(errorMessage(error).contains("expected string"))
        }
    }

    func testCompactRejectsUnknownOptions() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        XCTAssertThrowsError(try db.compact(options: ["targetRowsPerFrament": 512])) { error in
            XCTAssertTrue(errorMessage(error).contains("unknown compact option"))
        }
    }

    func testCleanupRejectsUnknownOptions() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        XCTAssertThrowsError(try db.cleanup(options: ["retainTxVersionz": 10])) { error in
            XCTAssertTrue(errorMessage(error).contains("unknown cleanup option"))
        }
    }

    func testCreateRejectsInvalidSchema() {
        let dbPath = tempPath(prefix: "nanograph-swift-invalid-schema")
        defer { cleanup(dbPath: dbPath) }

        XCTAssertThrowsError(
            try Database.create(
                dbPath: dbPath,
                schemaSource: "node Person { name: }"
            )
        ) { error in
            XCTAssertFalse(errorMessage(error).isEmpty)
        }
    }

    func testOpenRejectsMissingDatabase() {
        let missingPath = tempPath(prefix: "nanograph-swift-missing")
        XCTAssertThrowsError(try Database.open(dbPath: missingPath)) { error in
            XCTAssertFalse(errorMessage(error).isEmpty)
        }
    }

    func testLoadRejectsInvalidDataSource() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        XCTAssertThrowsError(try db.load(dataSource: "not-jsonl", mode: .append)) { error in
            XCTAssertFalse(errorMessage(error).isEmpty)
        }
    }

    func testRunRejectsNonJSONParams() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        XCTAssertThrowsError(
            try db.run(querySource: queries, queryName: "byRole", params: ["role": Date()])
        ) { error in
            XCTAssertTrue(errorMessage(error).contains("Value is not valid JSON"))
        }
    }

    func testTypedCheckAndDescribeDecode() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        let checks = try db.check([CheckRow].self, querySource: queries)
        XCTAssertEqual(checks.count, 2)
        XCTAssertTrue(checks.allSatisfy { $0.status == "ok" })

        let describe = try db.describe(DescribeResult.self)
        XCTAssertEqual(describe.nodeTypes.first?.name, "Person")
        XCTAssertEqual(describe.edgeTypes.count, 0)
    }

    func testDeinitWithoutClose() throws {
        let dbPath = tempPath(prefix: "nanograph-swift-deinit")
        do {
            let db = try Database.create(dbPath: dbPath, schemaSource: schema)
            try db.load(dataSource: data, mode: .overwrite)
        }
        cleanup(dbPath: dbPath)
    }

    func testCheckReturnsStatuses() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        let raw = try db.check(querySource: queries)
        let checks = try castRows(raw)
        XCTAssertEqual(checks.count, 2)
        XCTAssertTrue(checks.allSatisfy { ($0["status"] as? String) == "ok" })
        XCTAssertEqual((checks[0]["kind"] as? String), "read")
    }

    func testDescribeReturnsSchema() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        let raw = try db.describe()
        let describe = try castObject(raw)
        let nodeTypes = try XCTUnwrap(describe["nodeTypes"] as? [[String: Any]])
        let person = try XCTUnwrap(nodeTypes.first { ($0["name"] as? String) == "Person" })
        let properties = try XCTUnwrap(person["properties"] as? [[String: Any]])
        XCTAssertTrue(properties.contains { ($0["name"] as? String) == "name" })
    }

    func testDoctorReturnsHealthReport() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        let raw = try db.doctor()
        let report = try castObject(raw)
        XCTAssertEqual(report["healthy"] as? Bool, true)
        XCTAssertNotNil(report["datasetsChecked"] as? Int)
        XCTAssertNotNil(report["txRows"] as? Int)
        XCTAssertNotNil(report["cdcRows"] as? Int)
    }

    func testOpenPreservesData() throws {
        let (db, dbPath) = try freshDatabase()
        try db.close()

        let reopened = try Database.open(dbPath: dbPath)
        defer {
            try? reopened.close()
            cleanup(dbPath: dbPath)
        }

        let raw = try reopened.run(querySource: queries, queryName: "allPeople")
        let rows = try castRows(raw)
        XCTAssertEqual(rows.count, 2)
    }

    func testCompactAndCleanupDefaultOptions() throws {
        let (db, dbPath) = try freshDatabase()
        defer { cleanup(dbPath: dbPath) }
        defer { try? db.close() }

        let compactRaw = try db.compact()
        let compact = try castObject(compactRaw)
        XCTAssertNotNil(compact["datasetsConsidered"] as? Int)
        XCTAssertNotNil(compact["datasetsCompacted"] as? Int)
        XCTAssertNotNil(compact["manifestCommitted"] as? Bool)

        let cleanupRaw = try db.cleanup()
        let cleanup = try castObject(cleanupRaw)
        XCTAssertNotNil(cleanup["txRowsRemoved"] as? Int)
        XCTAssertNotNil(cleanup["datasetsCleaned"] as? Int)
        XCTAssertNotNil(cleanup["datasetBytesRemoved"] as? Int)
    }

    private func freshDatabase() throws -> (Database, String) {
        let dbPath = tempPath(prefix: "nanograph-swift")
        let db = try Database.create(dbPath: dbPath, schemaSource: schema)
        try db.load(dataSource: data, mode: .overwrite)
        return (db, dbPath)
    }

    private func tempPath(prefix: String) -> String {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("\(prefix)-\(UUID().uuidString)")
            .path
    }

    private func castRows(_ value: Any) throws -> [[String: Any]] {
        try XCTUnwrap(value as? [[String: Any]], "Expected [[String: Any]], got \(type(of: value))")
    }

    private func castObject(_ value: Any) throws -> [String: Any] {
        try XCTUnwrap(value as? [String: Any], "Expected [String: Any], got \(type(of: value))")
    }

    private func errorMessage(_ error: Error) -> String {
        if let nanoError = error as? NanoGraphError,
           case .message(let message) = nanoError
        {
            return message
        }
        return String(describing: error)
    }

    private func cleanup(dbPath: String) {
        try? FileManager.default.removeItem(atPath: dbPath)
    }
}
