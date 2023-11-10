import Dispatch
import NIOCore

// performs blocking calls outside of cooperative thread pool
internal func performBlockingCall<T>(queue: DispatchQueue, body: @escaping () -> T) async -> T {
    await withCheckedContinuation { continuation in
        queue.async {
            continuation.resume(returning: body())
        }
    }
}

// performs blocking calls outside of cooperative thread pool
internal func performBlockingCall<T>(queue: DispatchQueue, body: @escaping () throws -> T) async throws -> T {
    try await withCheckedThrowingContinuation { continuation in
        queue.async {
            do {
                continuation.resume(returning: try body())
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }
}

// TODO: remove
/* typealias Producer = NIOAsyncSequenceProducer<
KafkaProducerEvent,
NIOAsyncSequenceProducerBackPressureStrategies.NoBackPressure,
KafkaProducerCloseOnTerminate
>*/
//@discardableResult
//internal func yield<Event, BackPressure, Delegate>(message: Event, to continuation: NIOAsyncSequenceProducer<Event, BackPressure, Delegate>.Source?) async -> Bool {
//    guard let continuation else {
//        return false
//    }
//    while true {
//        let result = continuation.yield(message)
//        switch result {
//        case .dropped:
//            // Stream is closed
//            return false
//        case .produceMore:
//            // Here we can know how many slots remains in the stream
//            return true
//        case .dropped:
//            // Here we can know what message has beed dropped
//            await Task.yield()
//            continue
//        @unknown default:
//            fatalError("Runtime error: unknown case in \(#function), \(#file):\(#line)")
//        }
//    }
//}
//
//@discardableResult
//internal func yield<Event, Failure, BackPressure, Delegate>(message: Event, to continuation: NIOThrowingAsyncSequenceProducer<Event, Failure, BackPressure, Delegate>.Source?) async -> Bool {
//    guard let continuation else {
//        return false
//    }
//    while true {
//        let result = continuation.yield(message)
//        switch result {
//        case .terminated:
//            // Stream is closed
//            return false
//        case .enqueued:
//            // Here we can know how many slots remains in the stream
//            return true
//        case .dropped:
//            // Here we can know what message has beed dropped
//            await Task.yield()
//            continue
//        @unknown default:
//            fatalError("Runtime error: unknown case in \(#function), \(#file):\(#line)")
//        }
//    }
//}
