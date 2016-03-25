namespace cpp rocksdb.replication
namespace py rocksdb.replication

enum ErrorCode {
  SUCCESS = 0,
  // relication master has no data more recent than requested.
  NO_NEW_DATA,
  UPSTREAM_READ_ERROR,
  UPSTREAM_WRITE_ERROR,
  DOWNSTREAM_READ_ERROR,
  DOWNSTREAM_WRITE_ERROR,
  CKPT_ERROR,
  HTTP_SERVER_START_FAILURE,
}

enum RpcReplMode {
  SemiSync = 0,
  Async = 1,
}

// Rocksdb operation return status.
struct Status {
  // Same code as rocksdb::Status.code
  1: i32 code;
}

struct RocksdbOpResponse {
  1: Status status;
  2: string data;
}

// An object of Update represent a WAL log entry applied to rocksdb.
// A log entry may include multiple write-batches.
struct Update {
  // beginning sequence no. of all write-batches encoded in the data.
  1: i64 sequence;

  // time stamp of the update.
  2: i64 timestamp;

  // string representation of this udpate. It is taken from WriteBatch.rep_.
  3: string data;
}

// A list of update objects.
struct Updates {
  1: required list<Update> updates;
}

// "pull from upstream" request.
struct PullRequest {
  // want to pull updates later than (not inclusive) this seq no.
  1: required i64 sequence;

  // expect to receive no more than these many updates.
  2: optional i32 max_updates = 5000;
}

// Response to a "pull from upstream" request.
struct PullResponse {
  1: required ErrorCode result; 
  2: required list<Update> updates;
}

// Response to a "create checkpoint" request.
struct CkptResult {
  1: ErrorCode result;

  // checkpoint name.
  2: string name;

  // parent dir name of the checkpoint at owner rocksdb, without a trailing '/'.
  3: string dirname;

  // all files included in this checkpoint, rooted from "dirname".
  4: list<string> filenames;
}

// Info about a running http server.
struct HttpServerInfo {
  // If the struct is ok.
  1: ErrorCode state;

  // server listens at this port.
  2: i32 port;

  3: string address;

  4: string root_dir;
}

exception ReplException {
  1: required ErrorCode code,
  2: optional string msg,
}

service Replication {

  // A downstream tells upstream to establishe a replication connection in the
  // given replication mode.
  //
  // Upstream RPC server will start a replication-thread, connect to downstream RPC
  // server at given "address:port".
  //
  // When this method returns success, upstream has synced the downstream up to date,
  // and started replication in given mode.
  bool ConnectToUpstream(1:RpcReplMode mode, 2:string address, 3:i32 port);


  /////////////////////////////
  // Push-based Replication: semi-sync, or async mode.
  //
  // Upstream (master) pushes new updates to downstream (slaves).
  // Slave (downstream) accepts an update entry from master (upstream), and applies the update.
  // Return:
  //   true if the change is applied successfully, false otherwise.
  bool PushToDownstream(1:Update update) throws (1:ReplException e);

  bool PushRawToDownstream(1:i64 sequence, 2:string data) throws (1:ReplException e);

  bool PushMultipleToDownstream(1:Updates updates) throws (1:ReplException e);


  /////////////////////////////
  // Pull based replication always runs async mode.
  //
  // async mode: slaves pull udpates from master (upstream) starting from "sequence" number.
  // "count" is expected number of records to pull.
  // Return:
  //   earliest updates > "sequence" (not inclusive).
  //   These udpates are sorted by ascending seq number.
  PullResponse PullFromUpstream(1:PullRequest req)
    throws (1:ReplException e);

  /////////////////////////////
  // Rocksdb operations

  i64 GetSequenceNumber();

  RocksdbOpResponse Get(1:string key);

  RocksdbOpResponse Put(1:string key, 2:string data);

  CkptResult CreateCheckpoint(1:string name);

  void DeleteCheckpoint(1:string name);

  // Request this rpc server to start a http server to serve data from the given dir.
  HttpServerInfo StartHttpServer(1:string root_dir);

  // Stop http server that is bound to the  given dir.
  void StopHttpServer(1:string root_dir);

}
