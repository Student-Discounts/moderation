package org.studentdiscounts.media;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;

public final class ImagePipeline {
  public record CleanImage(byte[] png, int width, int height) {}

  public static final class RejectedImage extends RuntimeException {
    public RejectedImage(String reason) {
      super(reason);
    }
  }

  private final int maxDimension;
  private final long maxPixels;

  public ImagePipeline(int maxDimension, long maxPixels) {
    this.maxDimension = maxDimension;
    this.maxPixels = maxPixels;
  }

  public CleanImage sanitize(byte[] bytes, String filename, String mime) {
    if (filename == null
        || filename.length() > 200
        || filename.isBlank()
        || filename.indexOf('/') >= 0
        || filename.indexOf('\\') >= 0
        || filename.codePoints().anyMatch(Character::isISOControl))
      throw new RejectedImage("INVALID_FILENAME");
    String lower = filename.toLowerCase(Locale.ROOT);
    boolean png =
        bytes.length >= 8
            && bytes[0] == (byte) 137
            && bytes[1] == 'P'
            && bytes[2] == 'N'
            && bytes[3] == 'G'
            && bytes[4] == 13
            && bytes[5] == 10
            && bytes[6] == 26
            && bytes[7] == 10;
    boolean jpeg =
        bytes.length >= 3
            && bytes[0] == (byte) 255
            && bytes[1] == (byte) 216
            && bytes[2] == (byte) 255;
    if (!(png && lower.endsWith(".png") && "image/png".equals(mime))
        && !(jpeg
            && (lower.endsWith(".jpg") || lower.endsWith(".jpeg"))
            && "image/jpeg".equals(mime))) throw new RejectedImage("TYPE_MISMATCH");
    // In-memory streams avoid ImageIO's default disk cache. Header dimensions are read before
    // read().
    try (var input = new MemoryCacheImageInputStream(new ByteArrayInputStream(bytes))) {
      var readers = ImageIO.getImageReadersByFormatName(png ? "png" : "jpeg");
      if (!readers.hasNext())
        throw new IllegalStateException("Required JDK image codec unavailable");
      ImageReader reader = readers.next();
      boolean[] warning = {false};
      reader.addIIOReadWarningListener((source, message) -> warning[0] = true);
      try {
        reader.setInput(input, true, true);
        int width = reader.getWidth(0), height = reader.getHeight(0);
        if (width <= 0
            || height <= 0
            || width > maxDimension
            || height > maxDimension
            || (long) width * height > maxPixels) throw new RejectedImage("PIXEL_LIMIT");
        BufferedImage decoded = reader.read(0);
        if (warning[0]
            || decoded == null
            || decoded.getWidth() != width
            || decoded.getHeight() != height) throw new RejectedImage("CORRUPT_IMAGE");
        // Fresh pixel buffer + fresh PNG writer: no original metadata or extra frames are copied.
        BufferedImage safe = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D graphics = safe.createGraphics();
        try {
          graphics.drawImage(decoded, 0, 0, null);
        } finally {
          graphics.dispose();
          decoded.flush();
        }
        try (var output = new ByteArrayOutputStream();
            var imageOutput = new javax.imageio.stream.MemoryCacheImageOutputStream(output)) {
          var writers = ImageIO.getImageWritersByFormatName("png");
          if (!writers.hasNext())
            throw new IllegalStateException("Required JDK PNG encoder unavailable");
          var writer = writers.next();
          try {
            writer.setOutput(imageOutput);
            writer.write(
                null, new javax.imageio.IIOImage(safe, null, null), writer.getDefaultWriteParam());
            imageOutput.flush();
            return new CleanImage(output.toByteArray(), width, height);
          } finally {
            writer.dispose();
          }
        } finally {
          safe.flush();
        }
      } finally {
        reader.dispose();
      }
    } catch (IOException | IllegalArgumentException e) {
      throw new RejectedImage("CORRUPT_IMAGE");
    }
  }
}
