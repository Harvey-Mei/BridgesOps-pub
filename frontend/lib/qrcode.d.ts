declare module "qrcode" {
  interface QRCodeCanvasOptions {
    width?: number
    margin?: number
  }

  const QRCode: {
    toCanvas(
      canvas: HTMLCanvasElement,
      text: string,
      options?: QRCodeCanvasOptions,
    ): Promise<void>
  }

  export default QRCode
}
