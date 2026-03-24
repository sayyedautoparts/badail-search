//
//  BarcodeInputNormalizer.swift
//  Filter & verify camera/scanner strings before sending to FastAPI (Hyundai/Kia style).
//

import Foundation

public enum BarcodeInputNormalizer {

    /// Arabic / Persian digits → ASCII, then ASCII digits only (spaces and punctuation removed).
    public static func normalizeBarcode(_ raw: String) -> String {
        var out = ""
        out.reserveCapacity(raw.count)
        for s in raw.unicodeScalars {
            switch s.value {
            case 0x30 ... 0x39:
                out.append(Character(s))
            case 0x0660 ... 0x0669:
                out.append(Character(UnicodeScalar(0x30 + (s.value - 0x0660))!))
            case 0x06F0 ... 0x06F9:
                out.append(Character(UnicodeScalar(0x30 + (s.value - 0x06F0))!))
            default:
                break
            }
        }
        return out
    }

    /// Reject if too short or if after cleaning there are no digits and junk was present.
    public static func isAcceptableNumericBarcode(_ cleaned: String) -> Bool {
        guard cleaned.count >= 6 else { return false }
        if cleaned.count > 20 { return false }
        return cleaned.allSatisfy(\.isNumber)
    }

    /// Typical automotive numeric label (Hyundai/Kia): 8…13 digits after normalization.
    public static func isHyundaiStyleLength(_ cleaned: String) -> Bool {
        (8...13).contains(cleaned.count)
    }
}

// MARK: - Multi-scan + 1-digit noise rejection

/// Accept only when the same normalized string appears `requiredMatches` times in a row.
/// If the new scan differs from the **previous** raw scan by exactly one digit (same length), treat as noise and ignore.
public final class BarcodeScanGate {
    public var requiredMatches: Int
    public var minimumLength: Int
    private var streak: [String] = []
    private var lastRawNormalized: String?

    public init(requiredMatches: Int = 3, minimumLength: Int = 6) {
        self.requiredMatches = max(2, requiredMatches)
        self.minimumLength = minimumLength
    }

    public func reset() {
        streak.removeAll()
        lastRawNormalized = nil
    }

    /// Returns normalized barcode when accepted; `nil` otherwise.
    public func push(scannedRaw: String) -> String? {
        let code = BarcodeInputNormalizer.normalizeBarcode(scannedRaw)
        guard BarcodeInputNormalizer.isAcceptableNumericBarcode(code) else {
            streak.removeAll()
            lastRawNormalized = nil
            return nil
        }

        if let prev = lastRawNormalized, prev.count == code.count, hammingDistance(prev, code) == 1 {
            return nil
        }
        lastRawNormalized = code

        if streak.last == code {
            streak.append(code)
        } else {
            streak = [code]
        }

        if streak.count >= requiredMatches {
            streak.removeAll()
            lastRawNormalized = nil
            return code
        }
        return nil
    }

    private func hammingDistance(_ a: String, _ b: String) -> Int {
        guard a.count == b.count else { return Int.max }
        return zip(a, b).filter { $0 != $1 }.count
    }
}

/// Sliding window: last N scans must all be the same normalized value (simple alternative).
public final class BarcodeRepeatValidator {
    private var buffer: [String] = []
    private let window: Int

    public init(window: Int = 3) {
        self.window = max(2, window)
    }

    public func reset() {
        buffer.removeAll()
    }

    public func push(normalized: String) -> Bool {
        guard !normalized.isEmpty else { return false }
        buffer.append(normalized)
        if buffer.count > window { buffer.removeFirst() }
        guard buffer.count == window else { return false }
        return Set(buffer).count == 1
    }
}
