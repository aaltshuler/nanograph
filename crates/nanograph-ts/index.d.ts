/* @nanograph/sdk — TypeScript type definitions (hand-written) */

export interface PropDef {
  name: string;
  type: string;
  nullable: boolean;
  list?: boolean;
  key?: boolean;
  unique?: boolean;
  index?: boolean;
  enumValues?: string[];
  embedSource?: string;
}

export interface NodeType {
  name: string;
  typeId: number;
  properties: PropDef[];
}

export interface EdgeType {
  name: string;
  srcType: string;
  dstType: string;
  typeId: number;
  properties: PropDef[];
}

export interface DescribeResult {
  nodeTypes: NodeType[];
  edgeTypes: EdgeType[];
}

export interface CheckResult {
  name: string;
  kind: "read" | "mutation";
  status: "ok" | "error";
  error?: string;
}

export interface MutationResult {
  affectedNodes: number;
  affectedEdges: number;
}

export interface CompactOptions {
  targetRowsPerFragment?: number;
}

export interface CompactResult {
  datasetsConsidered: number;
  datasetsCompacted: number;
  fragmentsRemoved: number;
  fragmentsAdded: number;
  filesRemoved: number;
  filesAdded: number;
  manifestCommitted: boolean;
}

export interface CleanupOptions {
  retainTxVersions?: number;
}

export interface CleanupResult {
  txRowsRemoved: number;
  txRowsKept: number;
  cdcRowsRemoved: number;
  cdcRowsKept: number;
  datasetsCleaned: number;
  datasetOldVersionsRemoved: number;
  datasetBytesRemoved: number;
}

export interface DoctorResult {
  healthy: boolean;
  issues: string[];
  warnings: string[];
  datasetsChecked: number;
  txRows: number;
  cdcRows: number;
}

export class Database {
  /** Create a new database from a schema string. */
  static init(dbPath: string, schemaSource: string): Promise<Database>;

  /** Open an existing database. */
  static open(dbPath: string): Promise<Database>;

  /** Load JSONL data into the database. */
  load(dataSource: string, mode: "overwrite" | "append" | "merge"): Promise<void>;

  /**
   * Execute a named query from query source text.
   * Read queries return an array of row objects.
   * Mutation queries return `{ affectedNodes, affectedEdges }`.
   */
  run(
    querySource: string,
    queryName: string,
    params?: Record<string, unknown>,
  ): Promise<Record<string, unknown>[] | MutationResult>;

  /** Typecheck all queries against the database schema. */
  check(querySource: string): Promise<CheckResult[]>;

  /** Return schema introspection. */
  describe(): Promise<DescribeResult>;

  /** Compact Lance datasets to reduce fragmentation. */
  compact(options?: CompactOptions): Promise<CompactResult>;

  /** Clean up old dataset versions and prune logs. */
  cleanup(options?: CleanupOptions): Promise<CleanupResult>;

  /** Run health checks on the database. */
  doctor(): Promise<DoctorResult>;

  /** Close the database, releasing resources. */
  close(): void;
}
