#import <Foundation/Foundation.h>
#import <notify.h>
#import "RRWorkerClient.h"

#define PREFS_PATH @"/var/mobile/Library/Preferences/com.relayrpc.worker.plist"
#define PREFS_CHANGED "com.relayrpc.worker/prefsChanged"

// Default values - change these before building
#define DEFAULT_SERVER_URL @"ws://192.168.1.16:8080/api/v1/workers/ws"
#define DEFAULT_TOKEN @""

@interface RRTaskExecutor : NSObject <RRWorkerTaskHandler>
@end

@implementation RRTaskExecutor

- (void)handleTaskPayload:(NSDictionary *)payload
                   taskID:(NSString *)taskID
                attemptID:(NSString *)attemptID
               completion:(void (^)(BOOL success, NSDictionary *result, NSString *errorCode, NSString *errorMessage))completion {

    NSString *action = payload[@"action"] ?: @"unknown";
    NSLog(@"[RelayRPCWorker] executing action: %@", action);

    // ===== Synchronous example =====
    // Process task and return result immediately
    completion(YES, @{@"action": action, @"source": @"ios_device"}, nil, nil);

    // ===== Asynchronous example =====
    // Use this when task involves network requests, waiting for events, etc.
    // dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
    //     // ... do async work ...
    //     completion(YES, @{@"action": action, @"source": @"ios_device"}, nil, nil);
    //     // or on failure:
    //     // completion(NO, nil, @"TASK_FAILED", @"something went wrong");
    // });
}

@end

static RRTaskExecutor *executor;

static void loadAndStart(void) {
    NSDictionary *prefs = [NSDictionary dictionaryWithContentsOfFile:PREFS_PATH];

    NSString *serverURL = prefs[@"serverURL"] ?: DEFAULT_SERVER_URL;
    NSString *token = prefs[@"token"] ?: DEFAULT_TOKEN;
    BOOL enabled = prefs ? [prefs[@"enabled"] boolValue] : (DEFAULT_TOKEN.length > 0);

    if (enabled && serverURL.length > 0 && token.length > 0) {
        if (!executor) {
            executor = [[RRTaskExecutor alloc] init];
        }
        [RRWorkerClient shared].taskHandler = executor;
        [[RRWorkerClient shared] startWithServerURL:serverURL token:token];
    } else {
        [[RRWorkerClient shared] stop];
    }
}

%ctor {
    @autoreleasepool {
        NSLog(@"[RelayRPCWorker] tweak loaded");

        int token;
        notify_register_dispatch(PREFS_CHANGED, &token, dispatch_get_main_queue(), ^(int t) {
            NSLog(@"[RelayRPCWorker] preferences changed");
            loadAndStart();
        });

        dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(3 * NSEC_PER_SEC)), dispatch_get_main_queue(), ^{
            loadAndStart();
        });
    }
}
