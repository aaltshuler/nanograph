import CNanoGraph
import Foundation

public enum NanoGraphError: Error, LocalizedError {
    case message(String)

    public var errorDescription: String? {
        switch self {
        case .message(let message):
            return message
        }
    }
}

public enum LoadMode: String {
    case overwrite
    case append
    case merge
}

public final class Database {
    private var handle: OpaquePointer?

    private init(handle: OpaquePointer) {
        self.handle = handle
    }

    deinit {
        if let handle {
            nanograph_db_destroy(handle)
            self.handle = nil
        }
    }

    public static func create(dbPath: String, schemaSource: String) throws -> Database {
        let handle = dbPath.withCString { dbPathPtr in
            schemaSource.withCString { schemaPtr in
                nanograph_db_init(dbPathPtr, schemaPtr)
            }
        }
        guard let handle else {
            throw NanoGraphError.message(Self.lastErrorMessage())
        }
        return Database(handle: handle)
    }

    public static func open(dbPath: String) throws -> Database {
        let handle = dbPath.withCString { dbPathPtr in
            nanograph_db_open(dbPathPtr)
        }
        guard let handle else {
            throw NanoGraphError.message(Self.lastErrorMessage())
        }
        return Database(handle: handle)
    }

    public func close() throws {
        guard let handle else {
            return
        }
        let status = nanograph_db_close(handle)
        if status != 0 {
            throw NanoGraphError.message(Self.lastErrorMessage())
        }
        nanograph_db_destroy(handle)
        self.handle = nil
    }

    public func load(dataSource: String, mode: LoadMode) throws {
        let handle = try requireHandle()
        let status = dataSource.withCString { dataPtr in
            mode.rawValue.withCString { modePtr in
                nanograph_db_load(handle, dataPtr, modePtr)
            }
        }
        if status != 0 {
            throw NanoGraphError.message(Self.lastErrorMessage())
        }
    }

    public func run(
        querySource: String,
        queryName: String,
        params: Any? = nil
    ) throws -> Any {
        let paramsJSON = try encodeJSON(params)
        let handle = try requireHandle()
        let ptr = querySource.withCString { querySourcePtr in
            queryName.withCString { queryNamePtr in
                if let paramsJSON {
                    return paramsJSON.withCString { paramsPtr in
                        nanograph_db_run(handle, querySourcePtr, queryNamePtr, paramsPtr)
                    }
                }
                return nanograph_db_run(handle, querySourcePtr, queryNamePtr, nil)
            }
        }
        return try decodeOwnedJSONString(ptr)
    }

    public func check(querySource: String) throws -> Any {
        let handle = try requireHandle()
        let ptr = querySource.withCString { querySourcePtr in
            nanograph_db_check(handle, querySourcePtr)
        }
        return try decodeOwnedJSONString(ptr)
    }

    public func describe() throws -> Any {
        let handle = try requireHandle()
        let ptr = nanograph_db_describe(handle)
        return try decodeOwnedJSONString(ptr)
    }

    public func compact(options: Any? = nil) throws -> Any {
        let handle = try requireHandle()
        let optionsJSON = try encodeJSON(options)
        let ptr = if let optionsJSON {
            optionsJSON.withCString { optionsPtr in
                nanograph_db_compact(handle, optionsPtr)
            }
        } else {
            nanograph_db_compact(handle, nil)
        }
        return try decodeOwnedJSONString(ptr)
    }

    public func cleanup(options: Any? = nil) throws -> Any {
        let handle = try requireHandle()
        let optionsJSON = try encodeJSON(options)
        let ptr = if let optionsJSON {
            optionsJSON.withCString { optionsPtr in
                nanograph_db_cleanup(handle, optionsPtr)
            }
        } else {
            nanograph_db_cleanup(handle, nil)
        }
        return try decodeOwnedJSONString(ptr)
    }

    public func doctor() throws -> Any {
        let handle = try requireHandle()
        let ptr = nanograph_db_doctor(handle)
        return try decodeOwnedJSONString(ptr)
    }

    public func run<T: Decodable>(
        _ type: T.Type,
        querySource: String,
        queryName: String,
        params: Any? = nil
    ) throws -> T {
        let raw = try run(querySource: querySource, queryName: queryName, params: params)
        return try decodeValue(type, from: raw)
    }

    public func check<T: Decodable>(_ type: T.Type, querySource: String) throws -> T {
        let raw = try check(querySource: querySource)
        return try decodeValue(type, from: raw)
    }

    public func describe<T: Decodable>(_ type: T.Type) throws -> T {
        let raw = try describe()
        return try decodeValue(type, from: raw)
    }

    private func requireHandle() throws -> OpaquePointer {
        guard let handle else {
            throw NanoGraphError.message("Database is closed")
        }
        return handle
    }

    private static func lastErrorMessage() -> String {
        guard let errPtr = nanograph_last_error_message() else {
            return "Unknown NanoGraph FFI error"
        }
        return String(cString: errPtr)
    }

    private func decodeOwnedJSONString(_ ptr: UnsafeMutablePointer<CChar>?) throws -> Any {
        guard let ptr else {
            throw NanoGraphError.message(Self.lastErrorMessage())
        }
        defer {
            nanograph_string_free(ptr)
        }
        let json = String(cString: ptr)
        return try decodeJSON(json)
    }
}

private func encodeJSON(_ value: Any?) throws -> String? {
    guard let value else {
        return nil
    }
    guard JSONSerialization.isValidJSONObject(value) else {
        throw NanoGraphError.message("Value is not valid JSON")
    }
    let data = try JSONSerialization.data(withJSONObject: value, options: [])
    guard let string = String(data: data, encoding: .utf8) else {
        throw NanoGraphError.message("Failed to encode JSON string")
    }
    return string
}

private func decodeJSON(_ json: String) throws -> Any {
    let data = Data(json.utf8)
    return try JSONSerialization.jsonObject(with: data, options: [])
}

private func decodeValue<T: Decodable>(_ type: T.Type, from value: Any) throws -> T {
    let data = try JSONSerialization.data(withJSONObject: value, options: [])
    return try JSONDecoder().decode(type, from: data)
}
