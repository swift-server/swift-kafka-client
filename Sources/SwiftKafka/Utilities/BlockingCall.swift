import Dispatch

// performs blocking calls outside of cooperative thread pool
internal func performBlockingCall<T>(queue: DispatchQueue, body: @escaping () -> T) async -> T {
    await withCheckedContinuation { continuation in
        queue.async {
            continuation.resume(returning: body())
        }
    }
}
