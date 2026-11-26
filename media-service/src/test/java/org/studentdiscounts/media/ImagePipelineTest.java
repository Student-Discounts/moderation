package org.studentdiscounts.media;

import static org.junit.jupiter.api.Assertions.*;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import javax.imageio.ImageIO;
import org.junit.jupiter.api.Test;

class ImagePipelineTest {
  private final ImagePipeline pipeline = new ImagePipeline(4096, 8_000_000);

  static byte[] image(String format) throws Exception {
    var image = new BufferedImage(3, 2, BufferedImage.TYPE_INT_RGB);
    image.setRGB(1, 1, 0x123456);
    var out = new ByteArrayOutputStream();
    assertTrue(ImageIO.write(image, format, out));
    return out.toByteArray();
  }

  @Test
  void decodesJpegAndProducesOnlyFreshPng() throws Exception {
    var clean = pipeline.sanitize(image("jpeg"), "photo.JPEG", "image/jpeg");
    assertEquals(3, clean.width());
    assertEquals(2, clean.height());
    assertEquals("PNG", new String(clean.png(), 1, 3, java.nio.charset.StandardCharsets.US_ASCII));
    assertNotNull(ImageIO.read(new ByteArrayInputStream(clean.png())));
  }

  @Test
  void rejectsMimeAndExtensionSpoofingAndPaths() throws Exception {
    byte[] png = image("png");
    for (String name :
        new String[] {"photo.jpg", "../photo.png", "C:\\photo.png", "photo.png.html"})
      assertThrows(
          ImagePipeline.RejectedImage.class, () -> pipeline.sanitize(png, name, "image/png"));
    assertThrows(
        ImagePipeline.RejectedImage.class, () -> pipeline.sanitize(png, "photo.png", "image/jpeg"));
    assertThrows(
        ImagePipeline.RejectedImage.class,
        () -> pipeline.sanitize("<svg/>".getBytes(), "photo.png", "image/png"));
  }

  @Test
  void rejectsHugeHeaderBeforeDecode() throws Exception {
    byte[] png = image("png");
    ByteBuffer.wrap(png).putInt(16, 100_000).putInt(20, 100_000);
    assertThrows(
        ImagePipeline.RejectedImage.class, () -> pipeline.sanitize(png, "photo.png", "image/png"));
    assertThrows(
        ImagePipeline.RejectedImage.class,
        () -> new ImagePipeline(4096, 5).sanitize(image("png"), "photo.png", "image/png"));
  }

  @Test
  void rejectsCorruptImageRatherThanReportingClean() throws Exception {
    byte[] png = image("png");
    byte[] truncated = java.util.Arrays.copyOf(png, 32);
    assertThrows(
        ImagePipeline.RejectedImage.class,
        () -> pipeline.sanitize(truncated, "photo.png", "image/png"));
  }

  @Test
  void rejectsTruncatedJpegEvenWhenDecoderCouldReturnPartialPixels() throws Exception {
    byte[] jpeg = image("jpeg");
    byte[] truncated = java.util.Arrays.copyOf(jpeg, jpeg.length - 8);
    assertThrows(
        ImagePipeline.RejectedImage.class,
        () -> pipeline.sanitize(truncated, "photo.jpg", "image/jpeg"));
  }

  @Test
  void stripsActualExifApplicationSegment() throws Exception {
    byte[] jpeg = image("jpeg");
    byte[] exif = "Exif\0\0PRIVATE-LOCATION".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    var input = new ByteArrayOutputStream();
    input.write(jpeg, 0, 2);
    input.write(
        new byte[] {
          (byte) 255, (byte) 225, (byte) ((exif.length + 2) >> 8), (byte) (exif.length + 2)
        });
    input.write(exif);
    input.write(jpeg, 2, jpeg.length - 2);
    var clean = pipeline.sanitize(input.toByteArray(), "photo.jpg", "image/jpeg");
    assertFalse(
        new String(clean.png(), java.nio.charset.StandardCharsets.ISO_8859_1)
            .contains("PRIVATE-LOCATION"));
  }

  @Test
  void stripsAppendedPrivateBytes() throws Exception {
    byte[] jpeg = image("jpeg");
    byte[] marker = "PRIVATE-EXIF-LOCATION".getBytes();
    byte[] input = java.util.Arrays.copyOf(jpeg, jpeg.length + marker.length);
    System.arraycopy(marker, 0, input, jpeg.length, marker.length);
    var clean = pipeline.sanitize(input, "photo.jpg", "image/jpeg");
    assertFalse(
        new String(clean.png(), java.nio.charset.StandardCharsets.ISO_8859_1)
            .contains("PRIVATE-EXIF"));
  }
}
