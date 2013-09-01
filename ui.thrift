namespace java fresto.format

struct UIEvent {
1: required string stage;
3: required string clientId;
4: optional string currentPlace;
5: required string uuid;
7: required string url;
8: optional string httpStatus;
9: required i64 timestamp;
11: optional i64 elapsedTime;
}
