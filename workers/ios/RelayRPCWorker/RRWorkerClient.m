#import "RRWorkerClient.h"

#define LOG_PREFIX @"[RelayRPCWorker]"

@interface RRWorkerClient () <NSURLSessionWebSocketDelegate>
@property (nonatomic, strong) NSURLSession *session;
@property (nonatomic, strong) NSURLSessionWebSocketTask *wsTask;
@property (nonatomic, strong) NSTimer *heartbeatTimer;
@property (nonatomic, strong) NSTimer *reconnectTimer;
@property (nonatomic, copy) NSString *serverURL;
@property (nonatomic, copy) NSString *token;
@property (nonatomic, assign) BOOL running;
// connected is written only on stateQueue but exposed read-only to callers on
// other threads, so it is atomic to avoid torn reads.
@property (atomic, assign, readwrite) BOOL connected;
// Serial queue that guards all mutable connection state (session, wsTask,
// timers, connected/running flags). Every access to that state runs here so
// the NSURLSession delegate callbacks (arbitrary background threads) and the
// public API can't race. Timers are scheduled on the main runloop but only
// ever fire blocks that hop back onto this queue.
@property (nonatomic, strong) dispatch_queue_t stateQueue;
@end

@implementation RRWorkerClient

+ (instancetype)shared {
    static RRWorkerClient *instance;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        instance = [[RRWorkerClient alloc] init];
    });
    return instance;
}

- (instancetype)init {
    self = [super init];
    if (self) {
        _stateQueue = dispatch_queue_create("com.relayrpc.worker.state", DISPATCH_QUEUE_SERIAL);
    }
    return self;
}

- (void)startWithServerURL:(NSString *)serverURL token:(NSString *)token {
    dispatch_async(_stateQueue, ^{
        [self stopLocked];
        self->_serverURL = serverURL;
        self->_token = token;
        self->_running = YES;
        [self connectLocked];
    });
}

- (void)stop {
    dispatch_async(_stateQueue, ^{
        [self stopLocked];
    });
}

- (void)stopLocked {
    self->_running = NO;
    [self disconnectLocked];
}

// connectLocked must run on stateQueue.
- (void)connectLocked {
    if (_connected || !_running) return;
    if (_serverURL.length == 0 || _token.length == 0) return;

    NSLog(@"%@ connecting to %@", LOG_PREFIX, _serverURL);

    NSURL *url = [NSURL URLWithString:_serverURL];
    if (!url) {
        NSLog(@"%@ invalid server URL", LOG_PREFIX);
        return;
    }

    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:url];
    [request setValue:[NSString stringWithFormat:@"Bearer %@", _token] forHTTPHeaderField:@"Authorization"];

    NSURLSessionConfiguration *config = [NSURLSessionConfiguration defaultSessionConfiguration];
    _session = [NSURLSession sessionWithConfiguration:config delegate:self delegateQueue:nil];
    _wsTask = [_session webSocketTaskWithRequest:request];
    [_wsTask resume];

    [self receiveMessage];
}

// disconnectLocked must run on stateQueue.
- (void)disconnectLocked {
    _connected = NO;
    [self cancelTimersOnMain];
    [_wsTask cancelWithCloseCode:NSURLSessionWebSocketCloseCodeNormalClosure reason:nil];
    _wsTask = nil;
    [_session invalidateAndCancel];
    _session = nil;
}

// Timers live on the main runloop; (in)validate them there.
- (void)cancelTimersOnMain {
    dispatch_async(dispatch_get_main_queue(), ^{
        [self->_heartbeatTimer invalidate];
        self->_heartbeatTimer = nil;
        [self->_reconnectTimer invalidate];
        self->_reconnectTimer = nil;
    });
}

// scheduleReconnect may be called from any thread; it hops to stateQueue to
// read running, then schedules the timer on the main runloop.
- (void)scheduleReconnect {
    dispatch_async(_stateQueue, ^{
        if (!self->_running || self->_connected) return;
        dispatch_async(dispatch_get_main_queue(), ^{
            [self->_reconnectTimer invalidate];
            self->_reconnectTimer = [NSTimer scheduledTimerWithTimeInterval:5.0 repeats:NO block:^(NSTimer *timer) {
                NSLog(@"%@ reconnecting...", LOG_PREFIX);
                dispatch_async(self->_stateQueue, ^{
                    [self connectLocked];
                });
            }];
        });
    });
}

- (void)startHeartbeat {
    dispatch_async(dispatch_get_main_queue(), ^{
        [self->_heartbeatTimer invalidate];
        self->_heartbeatTimer = [NSTimer scheduledTimerWithTimeInterval:10.0 repeats:YES block:^(NSTimer *timer) {
            [self sendHeartbeat];
        }];
    });
}

- (void)sendHeartbeat {
    [self sendJSON:@{
        @"type": @"heartbeat",
        @"ts": @((long)[[NSDate date] timeIntervalSince1970])
    }];
}

- (void)sendJSON:(NSDictionary *)dict {
    NSError *error;
    NSData *data = [NSJSONSerialization dataWithJSONObject:dict options:0 error:&error];
    if (error) return;

    NSString *str = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
    NSURLSessionWebSocketMessage *message = [[NSURLSessionWebSocketMessage alloc] initWithString:str];

    // Read _wsTask on the state queue so it can't be torn down mid-send.
    dispatch_async(_stateQueue, ^{
        NSURLSessionWebSocketTask *task = self->_wsTask;
        if (task == nil) return;
        [task sendMessage:message completionHandler:^(NSError *err) {
            if (err) {
                NSLog(@"%@ send error: %@", LOG_PREFIX, err.localizedDescription);
            }
        }];
    });
}

- (void)receiveMessage {
    // Capture the current task on the state queue; bail if the connection
    // changed underneath us so we don't keep reading a stale socket.
    dispatch_async(_stateQueue, ^{
        NSURLSessionWebSocketTask *task = self->_wsTask;
        if (task == nil) return;
        __weak typeof(self) weakSelf = self;
        [task receiveMessageWithCompletionHandler:^(NSURLSessionWebSocketMessage *message, NSError *error) {
            if (error) {
                NSLog(@"%@ receive error: %@", LOG_PREFIX, error.localizedDescription);
                dispatch_async(self->_stateQueue, ^{
                    self->_connected = NO;
                });
                [weakSelf scheduleReconnect];
                return;
            }

            if (message.type == NSURLSessionWebSocketMessageTypeString) {
                [weakSelf handleMessage:message.string];
            }

            [weakSelf receiveMessage];
        }];
    });
}

- (void)handleMessage:(NSString *)text {
    NSData *data = [text dataUsingEncoding:NSUTF8StringEncoding];
    NSDictionary *msg = [NSJSONSerialization JSONObjectWithData:data options:0 error:nil];
    if (!msg) return;

    NSString *type = msg[@"type"];
    if ([type isEqualToString:@"task"]) {
        NSLog(@"%@ received task: %@ attempt: %@", LOG_PREFIX, msg[@"task_id"], msg[@"attempt_id"]);
        [self handleTask:msg];
    }
}

- (void)handleTask:(NSDictionary *)taskMsg {
    NSString *taskID = taskMsg[@"task_id"];
    NSString *attemptID = taskMsg[@"attempt_id"];
    NSDictionary *payload = taskMsg[@"payload"];

    [self sendJSON:@{
        @"type": @"task_ack",
        @"task_id": taskID ?: @"",
        @"attempt_id": attemptID ?: @""
    }];
    NSLog(@"%@ sent ACK for %@", LOG_PREFIX, taskID);

    if (_taskHandler) {
        __weak typeof(self) weakSelf = self;
        [_taskHandler handleTaskPayload:payload taskID:taskID attemptID:attemptID completion:^(BOOL success, NSDictionary *result, NSString *errorCode, NSString *errorMessage) {
            NSMutableDictionary *resp = [@{
                @"type": @"task_result",
                @"task_id": taskID ?: @"",
                @"attempt_id": attemptID ?: @"",
                @"success": @(success),
            } mutableCopy];
            if (success && result) {
                resp[@"result"] = result;
            }
            if (!success) {
                resp[@"retryable"] = @YES;
                if (errorCode) resp[@"error_code"] = errorCode;
                if (errorMessage) resp[@"error_message"] = errorMessage;
            }
            [weakSelf sendJSON:resp];
            NSLog(@"%@ task %@ completed (success=%d)", LOG_PREFIX, taskID, success);
        }];
    } else {
        NSLog(@"%@ no taskHandler set, returning error for %@", LOG_PREFIX, taskID);
        [self sendJSON:@{
            @"type": @"task_result",
            @"task_id": taskID ?: @"",
            @"attempt_id": attemptID ?: @"",
            @"success": @NO,
            @"retryable": @YES,
            @"error_code": @"NO_HANDLER",
            @"error_message": @"no task handler configured"
        }];
    }
}

#pragma mark - NSURLSessionWebSocketDelegate
// Delegate callbacks arrive on arbitrary background threads, so all state
// mutation hops onto stateQueue.

- (void)URLSession:(NSURLSession *)session webSocketTask:(NSURLSessionWebSocketTask *)webSocketTask didOpenWithProtocol:(NSString *)protocol {
    NSLog(@"%@ connected!", LOG_PREFIX);
    dispatch_async(_stateQueue, ^{
        self->_connected = YES;
    });
    [self startHeartbeat];
}

- (void)URLSession:(NSURLSession *)session webSocketTask:(NSURLSessionWebSocketTask *)webSocketTask didCloseWithCode:(NSURLSessionWebSocketCloseCode)closeCode reason:(NSData *)reason {
    NSLog(@"%@ disconnected (code: %ld)", LOG_PREFIX, (long)closeCode);
    dispatch_async(_stateQueue, ^{
        self->_connected = NO;
    });
    [self cancelTimersOnMain];
    [self scheduleReconnect];
}

- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error {
    if (error) {
        NSLog(@"%@ connection error: %@", LOG_PREFIX, error.localizedDescription);
        dispatch_async(_stateQueue, ^{
            self->_connected = NO;
        });
        [self scheduleReconnect];
    }
}

@end
