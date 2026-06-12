#import <Foundation/Foundation.h>

@protocol RRWorkerTaskHandler <NSObject>
- (void)handleTaskPayload:(NSDictionary *)payload
                   taskID:(NSString *)taskID
                attemptID:(NSString *)attemptID
               completion:(void (^)(BOOL success, NSDictionary *result, NSString *errorCode, NSString *errorMessage))completion;
@end

@interface RRWorkerClient : NSObject

/// The task handler that executes incoming tasks. Held weakly to avoid the
/// client retaining (and pinning) the integrator's object.
///
/// IMPORTANT: the caller MUST keep a strong reference to the handler for as
/// long as the client is running (e.g. a strong property, an ivar, or a static
/// global). If the handler is deallocated, this reference becomes nil and every
/// incoming task fails with error_code "NO_HANDLER".
@property (nonatomic, weak) id<RRWorkerTaskHandler> taskHandler;

@property (atomic, assign, readonly) BOOL connected;

+ (instancetype)shared;

- (void)startWithServerURL:(NSString *)serverURL token:(NSString *)token;
- (void)stop;

@end
