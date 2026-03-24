//
//  BarcodeScannerSwiftUIView.swift
//  SwiftUI wrapper — add to the same iOS target as BarcodeScannerViewController.swift
//

import SwiftUI
import Vision

@available(iOS 15.0, *)
struct BarcodeScannerRepresentable: UIViewControllerRepresentable {
    var onCode: (String, VNBarcodeSymbology?) -> Void
    var requiredMatches: Int
    var minLength: Int
    var initialZoom: CGFloat

    init(
        requiredMatches: Int = 3,
        minLength: Int = 6,
        initialZoom: CGFloat = 1.6,
        onCode: @escaping (String, VNBarcodeSymbology?) -> Void
    ) {
        self.requiredMatches = requiredMatches
        self.minLength = minLength
        self.initialZoom = initialZoom
        self.onCode = onCode
    }

    func makeUIViewController(context: Context) -> BarcodeScannerViewController {
        let vc = BarcodeScannerViewController()
        vc.requiredConsecutiveMatches = requiredMatches
        vc.minimumBarcodeLength = minLength
        vc.preferredInitialZoomFactor = initialZoom
        vc.onBarcodeConfirmed = { code, sym in
            onCode(code, sym)
        }
        return vc
    }

    func updateUIViewController(_ uiViewController: BarcodeScannerViewController, context: Context) {}
}
