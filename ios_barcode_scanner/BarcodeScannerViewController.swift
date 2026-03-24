//
//  BarcodeScannerViewController.swift
//  High-accuracy 1D barcode scanning for iOS (AVFoundation + Vision + Core Image).
//
//  Info.plist: NSCameraUsageDescription
//
//  Target: iOS 15+ (adjust @available if needed for videoRotationAngle)
//

import UIKit
import AVFoundation
import Vision
import CoreImage
import CoreImage.CIFilterBuiltins

// MARK: - Thread-safe lock flag

private final class LockState {
    private let lock = NSLock()
    private var value = false
    func get() -> Bool {
        lock.lock(); defer { lock.unlock() }
        return value
    }
    func set(_ v: Bool) {
        lock.lock(); defer { lock.unlock() }
        value = v
    }
}

// MARK: - EAN-13 / UPC quick validation

enum BarcodeValidation {
    /// Reject obviously wrong reads; optional EAN-13 check digit.
    static func isPlausible(_ string: String, symbology: VNBarcodeSymbology?) -> Bool {
        let trimmed = string.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.count >= 4 else { return false }
        // Mostly digits for common retail / automotive labels
        let digits = trimmed.filter { $0.isNumber }
        if symbology == .ean13 || symbology == .upce {
            return digits.count == trimmed.count && (trimmed.count == 8 || trimmed.count == 12 || trimmed.count == 13)
        }
        if symbology == .code128 || symbology == .code39 {
            return trimmed.count >= 4
        }
        return true
    }

    /// EAN-13 check digit (12 digits + check). Returns true if length 13 and check matches.
    static func ean13CheckDigitValid(_ string: String) -> Bool {
        guard string.count == 13, string.allSatisfy({ $0.isNumber }) else { return true }
        let digits = string.compactMap { Int(String($0)) }
        guard digits.count == 13 else { return true }
        var sum = 0
        for i in 0..<12 {
            let m = (i % 2 == 0) ? 1 : 3
            sum += digits[i] * m
        }
        let check = (10 - (sum % 10)) % 10
        return check == digits[12]
    }
}

// MARK: - Debounce

final class BarcodeDebouncer {
    private let requiredMatches: Int
    private var recent: [String] = []
    private let queue = DispatchQueue(label: "barcode.debounce")

    init(requiredMatches: Int = 3) {
        self.requiredMatches = max(2, requiredMatches)
    }

    /// Returns payload when the same string was seen `requiredMatches` times in a row.
    func consider(_ raw: String) -> String? {
        queue.sync {
            let normalized = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !normalized.isEmpty else {
                recent.removeAll()
                return nil
            }
            if recent.last == normalized {
                recent.append(normalized)
            } else {
                recent = [normalized]
            }
            if recent.count >= requiredMatches {
                recent.removeAll()
                return normalized
            }
            return nil
        }
    }

    func reset() {
        queue.sync { recent.removeAll() }
    }
}

// MARK: - Main scanner

final class BarcodeScannerViewController: UIViewController {

    // MARK: Public callbacks / tuning

    var onBarcodeConfirmed: ((String, VNBarcodeSymbology?) -> Void)?
    var onScanningStateChanged: ((Bool) -> Void)?

    /// Minimum characters to accept (after trim).
    var minimumBarcodeLength: Int = 6
    /// Same payload must repeat this many times consecutively.
    var requiredConsecutiveMatches: Int = 3 {
        didSet { debouncer = BarcodeDebouncer(requiredMatches: requiredConsecutiveMatches) }
    }
    /// Vision ROI in normalized coords (origin bottom-left). Wide horizontal band in center.
    var scanRegionOfInterest: CGRect = CGRect(x: 0.06, y: 0.38, width: 0.88, height: 0.28)
    /// Target max processing rate (approximate).
    var maxProcessingFPS: Double = 8
    /// Enable extra edge emphasis (heavier CPU).
    var enableEdgeBlend: Bool = false
    /// Default ~1.6x for large labels (clamped to device limits).
    var preferredInitialZoomFactor: CGFloat = 1.6

    // MARK: UI

    private let previewView = UIView()
    private let overlayView = UIView()
    private let roiGuideLayer = CAShapeLayer()
    private let boundingBoxLayer = CAShapeLayer()
    private let statusLabel = UILabel()
    private let zoomLabel = UILabel()
    private let lockBadge = UILabel()

    // MARK: AV

    private let session = AVCaptureSession()
    private let sessionQueue = DispatchQueue(label: "barcode.session")
    private let processingQueue = DispatchQueue(label: "barcode.vision", qos: .userInitiated)
    private var videoOutput = AVCaptureVideoDataOutput()
    private var previewLayer: AVCaptureVideoPreviewLayer?
    private var device: AVCaptureDevice?
    private var videoConnection: AVCaptureConnection?

    // MARK: Vision / CI

    private let ciContext = CIContext(options: [.useSoftwareRenderer: false])
    private var barcodeRequest = VNDetectBarcodesRequest()
    private var debouncer = BarcodeDebouncer(requiredMatches: 3)
    private var lastProcessTime: CFTimeInterval = 0
    private let lockedState = LockState()
    /// Vision orientation for video buffers (updated from window scene).
    private var bufferVisionOrientation: CGImagePropertyOrientation = .right

    // MARK: Lifecycle

    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black
        setupUI()
        configureBarcodeRequest()
    }

    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        sessionQueue.async { self.configureSessionIfNeeded() }
    }

    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        sessionQueue.async {
            if self.session.isRunning { self.session.stopRunning() }
        }
    }

    override func viewDidLayoutSubviews() {
        super.viewDidLayoutSubviews()
        previewLayer?.frame = previewView.bounds
        updateROIGuidePath()
        if let io = view.window?.windowScene?.interfaceOrientation {
            bufferVisionOrientation = Self.visionOrientation(from: io)
        }
    }

    private static func visionOrientation(from io: UIInterfaceOrientation) -> CGImagePropertyOrientation {
        switch io {
        case .portrait: return .right
        case .portraitUpsideDown: return .left
        case .landscapeLeft: return .up
        case .landscapeRight: return .down
        default: return .right
        }
    }

    // MARK: Setup UI

    private func setupUI() {
        previewView.translatesAutoresizingMaskIntoConstraints = false
        overlayView.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(previewView)
        view.addSubview(overlayView)

        NSLayoutConstraint.activate([
            previewView.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor),
            previewView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            previewView.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            previewView.bottomAnchor.constraint(equalTo: view.bottomAnchor),

            overlayView.topAnchor.constraint(equalTo: previewView.topAnchor),
            overlayView.leadingAnchor.constraint(equalTo: previewView.leadingAnchor),
            overlayView.trailingAnchor.constraint(equalTo: previewView.trailingAnchor),
            overlayView.bottomAnchor.constraint(equalTo: previewView.bottomAnchor),
        ])

        overlayView.isUserInteractionEnabled = true
        overlayView.backgroundColor = .clear

        roiGuideLayer.strokeColor = UIColor.systemYellow.withAlphaComponent(0.85).cgColor
        roiGuideLayer.fillColor = UIColor.clear.cgColor
        roiGuideLayer.lineWidth = 2
        overlayView.layer.addSublayer(roiGuideLayer)

        boundingBoxLayer.strokeColor = UIColor.systemGreen.cgColor
        boundingBoxLayer.fillColor = UIColor.systemGreen.withAlphaComponent(0.12).cgColor
        boundingBoxLayer.lineWidth = 3
        boundingBoxLayer.isHidden = true
        overlayView.layer.addSublayer(boundingBoxLayer)

        statusLabel.translatesAutoresizingMaskIntoConstraints = false
        statusLabel.textColor = .white
        statusLabel.font = .systemFont(ofSize: 15, weight: .semibold)
        statusLabel.textAlignment = .center
        statusLabel.numberOfLines = 2
        statusLabel.text = "وجّه الباركود داخل الإطار"
        statusLabel.layer.shadowColor = UIColor.black.cgColor
        statusLabel.layer.shadowOpacity = 0.8
        statusLabel.layer.shadowRadius = 2
        statusLabel.layer.shadowOffset = .zero
        view.addSubview(statusLabel)

        zoomLabel.translatesAutoresizingMaskIntoConstraints = false
        zoomLabel.textColor = .white
        zoomLabel.font = .monospacedDigitSystemFont(ofSize: 13, weight: .medium)
        zoomLabel.textAlignment = .center
        zoomLabel.text = "Zoom: 1.0× — قرصة للتكبير"
        zoomLabel.layer.shadowColor = UIColor.black.cgColor
        zoomLabel.layer.shadowOpacity = 0.7
        zoomLabel.layer.shadowRadius = 2
        view.addSubview(zoomLabel)

        lockBadge.translatesAutoresizingMaskIntoConstraints = false
        lockBadge.text = "✓ تم القراءة"
        lockBadge.textColor = .systemGreen
        lockBadge.font = .systemFont(ofSize: 18, weight: .bold)
        lockBadge.backgroundColor = UIColor.black.withAlphaComponent(0.65)
        lockBadge.textAlignment = .center
        lockBadge.layer.cornerRadius = 10
        lockBadge.clipsToBounds = true
        lockBadge.isHidden = true
        view.addSubview(lockBadge)

        NSLayoutConstraint.activate([
            statusLabel.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor, constant: 12),
            statusLabel.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 16),
            statusLabel.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -16),

            zoomLabel.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -16),
            zoomLabel.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 16),
            zoomLabel.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -16),

            lockBadge.centerXAnchor.constraint(equalTo: view.centerXAnchor),
            lockBadge.centerYAnchor.constraint(equalTo: view.centerYAnchor, constant: -40),
            lockBadge.widthAnchor.constraint(greaterThanOrEqualToConstant: 160),
            lockBadge.heightAnchor.constraint(equalToConstant: 44),
        ])

        let pinch = UIPinchGestureRecognizer(target: self, action: #selector(handlePinch(_:)))
        overlayView.addGestureRecognizer(pinch)
    }

    private func updateROIGuidePath() {
        let b = overlayView.bounds
        guard b.width > 0, b.height > 0 else { return }
        // Vision ROI is normalized, origin bottom-left → UIKit top-left
        let vx = scanRegionOfInterest.origin.x
        let vyBottom = scanRegionOfInterest.origin.y
        let vw = scanRegionOfInterest.width
        let vh = scanRegionOfInterest.height
        let rect = CGRect(
            x: vx * b.width,
            y: (1 - vyBottom - vh) * b.height,
            width: vw * b.width,
            height: vh * b.height
        )
        let path = UIBezierPath(roundedRect: rect, cornerRadius: 8)
        roiGuideLayer.path = path.cgPath
        roiGuideLayer.frame = b
    }

    // MARK: Session

    private func configureBarcodeRequest() {
        barcodeRequest.symbologies = [
            .code128,
            .code39,
            .ean13,
            .ean8,
            .upce,
            .code93,
            .i2of5,
        ]
        barcodeRequest.regionOfInterest = scanRegionOfInterest
    }

    private func configureSessionIfNeeded() {
        session.beginConfiguration()
        session.sessionPreset = .hd1920x1080
        if session.canSetSessionPreset(.hd4K3840x2160) {
            session.sessionPreset = .hd4K3840x2160
        }

        guard let dev = AVCaptureDevice.default(.builtInWideAngleCamera, for: .video, position: .back) else {
            session.commitConfiguration()
            DispatchQueue.main.async {
                self.statusLabel.text = "الكاميرا غير متوفرة"
            }
            return
        }

        do {
            try dev.lockForConfiguration()
            if dev.isFocusModeSupported(.continuousAutoFocus) {
                dev.focusMode = .continuousAutoFocus
            }
            if dev.isExposureModeSupported(.continuousAutoExposure) {
                dev.exposureMode = .continuousAutoExposure
            }
            if dev.isAutoFocusRangeRestrictionSupported {
                dev.autoFocusRangeRestriction = .none
            }
            dev.isSubjectAreaChangeMonitoringEnabled = true
            let maxZ = min(dev.activeFormat.videoMaxZoomFactor, 4.0)
            let z = min(max(preferredInitialZoomFactor, 1.0), maxZ)
            dev.videoZoomFactor = z
            dev.unlockForConfiguration()
        } catch {}

        device = dev

        session.inputs.forEach { session.removeInput($0) }
        session.outputs.forEach { session.removeOutput($0) }

        do {
            let input = try AVCaptureDeviceInput(device: dev)
            if session.canAddInput(input) { session.addInput(input) }
        } catch {
            session.commitConfiguration()
            return
        }

        videoOutput.videoSettings = [
            kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA
        ]
        videoOutput.alwaysDiscardsLateVideoFrames = true
        videoOutput.setSampleBufferDelegate(self, queue: processingQueue)

        if session.canAddOutput(videoOutput) {
            session.addOutput(videoOutput)
        }

        if let conn = videoOutput.connection(with: .video) {
            videoConnection = conn
            if #available(iOS 17.0, *) {
                if conn.isVideoRotationAngleSupported(90) {
                    conn.videoRotationAngle = 90
                }
            } else {
                if conn.isVideoOrientationSupported {
                    conn.videoOrientation = .portrait
                }
            }
        }

        session.commitConfiguration()

        let layer = AVCaptureVideoPreviewLayer(session: session)
        layer.videoGravity = .resizeAspectFill
        DispatchQueue.main.async {
            self.previewLayer = layer
            layer.frame = self.previewView.bounds
            self.previewView.layer.insertSublayer(layer, at: 0)
            self.updateZoomLabel()
        }

        session.startRunning()
    }

    @objc private func handlePinch(_ gr: UIPinchGestureRecognizer) {
        guard let dev = device else { return }
        if gr.state == .began || gr.state == .changed {
            sessionQueue.async {
                do {
                    try dev.lockForConfiguration()
                    let maxZ = min(dev.activeFormat.videoMaxZoomFactor, 4.0)
                    var z = dev.videoZoomFactor * gr.scale
                    z = min(max(z, 1.0), maxZ)
                    dev.videoZoomFactor = z
                    gr.scale = 1
                    dev.unlockForConfiguration()
                    DispatchQueue.main.async { self.updateZoomLabel() }
                } catch {}
            }
        }
    }

    private func updateZoomLabel() {
        guard let dev = device else { return }
        zoomLabel.text = String(format: "Zoom: %.2f× — قرصة للتكبير", dev.videoZoomFactor)
    }

    // MARK: Preprocess + Vision

    private func preprocess(_ pixelBuffer: CVPixelBuffer) -> CVPixelBuffer {
        let image = CIImage(cvPixelBuffer: pixelBuffer)
        let w = CVPixelBufferGetWidth(pixelBuffer)
        let h = CVPixelBufferGetHeight(pixelBuffer)
        let extent = CGRect(x: 0, y: 0, width: w, height: h)

        let color = CIFilter.colorControls()
        color.inputImage = image
        color.contrast = 1.35
        color.brightness = 0.02
        color.saturation = 0.0

        var current = (color.outputImage ?? image).cropped(to: extent)

        let sharpen = CIFilter.sharpenLuminance()
        sharpen.inputImage = current
        sharpen.sharpness = 0.9
        if let out = sharpen.outputImage?.cropped(to: extent) {
            current = out
        }

        if enableEdgeBlend, let edges = CIFilter(name: "CIEdges") {
            edges.setValue(current, forKey: kCIInputImageKey)
            edges.setValue(2.0, forKey: kCIInputIntensityKey)
            if let eOut = edges.outputImage?.cropped(to: extent) {
                let blend = CIFilter(name: "CISourceOverCompositing")!
                blend.setValue(eOut, forKey: kCIInputImageKey)
                blend.setValue(current, forKey: kCIInputBackgroundImageKey)
                if let bOut = blend.outputImage?.cropped(to: extent) {
                    current = bOut
                }
            }
        }

        let attrs: [String: Any] = [
            kCVPixelBufferCGImageCompatibilityKey as String: true,
            kCVPixelBufferCGBitmapContextCompatibilityKey as String: true,
        ]
        var outBuf: CVPixelBuffer?
        CVPixelBufferCreate(kCFAllocatorDefault, w, h, kCVPixelFormatType_32BGRA, attrs as CFDictionary, &outBuf)
        guard let dst = outBuf else { return pixelBuffer }
        ciContext.render(current, to: dst, bounds: extent, colorSpace: CGColorSpaceCreateDeviceRGB())
        return dst
    }

    private func handleObservations(_ observations: [VNBarcodeObservation]) {
        guard !lockedState.get() else { return }
        guard let best = observations.max(by: { a, b in a.confidence < b.confidence }),
              let payload = best.payloadStringValue else {
            DispatchQueue.main.async {
                self.boundingBoxLayer.isHidden = true
                self.onScanningStateChanged?(false)
            }
            return
        }

        let sym = best.symbology
        guard payload.count >= minimumBarcodeLength else { return }
        guard BarcodeValidation.isPlausible(payload, symbology: sym) else { return }
        if sym == .ean13 && !BarcodeValidation.ean13CheckDigitValid(payload) { return }

        DispatchQueue.main.async {
            self.drawBoundingBox(best.boundingBox)
            self.onScanningStateChanged?(true)
        }

        if let confirmed = debouncer.consider(payload) {
            lockedState.set(true)
            DispatchQueue.main.async {
                self.lockBadge.isHidden = false
                self.statusLabel.text = confirmed
                self.onBarcodeConfirmed?(confirmed, sym)
            }
            sessionQueue.async {
                if self.session.isRunning { self.session.stopRunning() }
            }
        }
    }

    private func drawBoundingBox(_ box: CGRect) {
        // Vision: normalized, origin bottom-left
        let b = overlayView.bounds
        guard b.width > 0 else { return }
        let x = box.origin.x * b.width
        let y = (1 - box.origin.y - box.height) * b.height
        let w = box.width * b.width
        let h = box.height * b.height
        let rect = CGRect(x: x, y: y, width: w, height: h).insetBy(dx: -4, dy: -4)
        let path = UIBezierPath(roundedRect: rect, cornerRadius: 4)
        boundingBoxLayer.path = path.cgPath
        boundingBoxLayer.frame = b
        boundingBoxLayer.isHidden = false
    }

    func resetScanner() {
        debouncer.reset()
        lockedState.set(false)
        DispatchQueue.main.async {
            self.lockBadge.isHidden = true
            self.boundingBoxLayer.isHidden = true
            self.statusLabel.text = "وجّه الباركود داخل الإطار"
        }
        sessionQueue.async {
            if !self.session.isRunning { self.session.startRunning() }
        }
    }
}

// MARK: - Sample buffer delegate

extension BarcodeScannerViewController: AVCaptureVideoDataOutputSampleBufferDelegate {
    func captureOutput(_ output: AVCaptureOutput, didOutput sampleBuffer: CMSampleBuffer, from connection: AVCaptureConnection) {
        guard !lockedState.get() else { return }
        let now = CACurrentMediaTime()
        let minInterval = 1.0 / maxProcessingFPS
        guard now - lastProcessTime >= minInterval else { return }
        lastProcessTime = now

        guard let buf = CMSampleBufferGetImageBuffer(sampleBuffer) else { return }
        let processed = preprocess(buf)

        barcodeRequest.regionOfInterest = scanRegionOfInterest
        let handler = VNImageRequestHandler(cvPixelBuffer: processed, orientation: bufferVisionOrientation, options: [:])
        do {
            try handler.perform([barcodeRequest])
            let obs = barcodeRequest.results as? [VNBarcodeObservation] ?? []
            handleObservations(obs)
        } catch {
            DispatchQueue.main.async { self.boundingBoxLayer.isHidden = true }
        }
    }
}
