package com.github.toyset.joomtest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

public class FileUtil {

    private static class RandomAccessFileInStream extends InputStream {

        private final RandomAccessFile source;
        
        public RandomAccessFileInStream(RandomAccessFile source) {
            this.source = source;
        }

        @Override
        public void close() throws IOException {
            source.close();
        }

        @Override
        public int read() throws IOException {
            return source.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return source.read(b, off, len);
        }

        @Override
        public int read(byte[] b) throws IOException {
            return source.read(b);
        }

        @Override
        public long skip(long n) throws IOException {

            long pos = source.getFilePointer() + n;
            if (n > source.length()) {
                n = source.length() - source.getFilePointer();
                source.seek(source.length());
            } else {
                source.seek(pos);
            }
            
            return n;
        }
    }
    
    private static class RandomAccessFileOutStream extends OutputStream {

        private final RandomAccessFile dest;
        
        public RandomAccessFileOutStream(RandomAccessFile dest) {
            this.dest = dest;
        }

        @Override
        public void close() throws IOException {
            dest.close();
        }

        @Override
        public void flush() throws IOException {
            // Do nothing
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            dest.write(b, off, len);
        }

        @Override
        public void write(byte[] b) throws IOException {
            dest.write(b);
        }

        @Override
        public void write(int b) throws IOException {
            dest.write(b);
        }
    }
    
    private static class FileBuffer {
        public File file;
        public RandomAccessFile randomAccess;
        
        public BufferedReader reader;
        public BufferedWriter writer; 
    }
    
    
    private static final int BUFFERS_SIZE = 4;
    
    
    public static void sortLines(File in, File out, Charset charset) throws IOException {
        sortLines(in, out, charset, String::compareTo);
    }
    
    public static void sortLines(File in, File out, Charset charset, Comparator<String> comparator) throws IOException {
        
        File outDir = out.getParentFile();
        FileBuffer[] buffers = new FileBuffer[BUFFERS_SIZE];
        
        try {
            for (int i = 0; i < BUFFERS_SIZE; ++i) {
                buffers[i] = createBuffer(outDir, out.getName() + "-", charset);
            }
            
            long blockCount = 0;
            try (BufferedReader inReader =
                    new BufferedReader(new InputStreamReader(new FileInputStream(in), charset))) {
                
                Iterator<String> strIt = inReader.lines().iterator();
                while (strIt.hasNext()) {
                    buffers[(int) (blockCount % 2)].writer.append(strIt.next());
                    buffers[(int) (blockCount % 2)].writer.newLine();
                    buffers[(int) (blockCount % 2)].writer.flush();
                    blockCount++;
                }
            }
            
            int step = 0;
            long blockSize = 1;
            while (blockCount > 1) {
                
                FileBuffer inLeft = buffers[2*step];
                FileBuffer inRight = buffers[2*step + 1];
                
                inLeft.randomAccess.seek(0);
                inRight.randomAccess.seek(0);
                
                Iterator<String> inLeftIt = inLeft.reader.lines().iterator();
                Iterator<String> inRightIt = inRight.reader.lines().iterator();
                
                step = (step + 1) % 2;
                
                blockCount = 0;
                while (inLeftIt.hasNext() || inRightIt.hasNext()) {
                    merge(inLeftIt, inRightIt, buffers[2*step + (int) (blockCount % 2)], blockSize, comparator);
                    blockCount++;
                }
                
                inLeft.randomAccess.setLength(0);
                inRight.randomAccess.setLength(0);
                
                blockSize *= 2;
            }
            
            FileBuffer outBuffer = buffers[2*step];
            
            outBuffer.randomAccess.getChannel().force(true);
            outBuffer.randomAccess.close();
            
            if (!outBuffer.file.renameTo(out)) {
                throw new IOException("Can't create file");
            }
            
            outBuffer.file = null;
        } finally {
            Stream.of(buffers).forEach(buffer -> {
                if (buffer != null) {
                    closeQuietly(buffer.randomAccess);
                    deleteQuietly(buffer.file);
                }
            });
        }
    }
    
    private static boolean merge(Iterator<String> leftIt, Iterator<String> rightIt,
            FileBuffer out, long blockSize, Comparator<String> comparator) throws IOException {
        
        boolean result = false;
        
        long readLeft = 0;
        long readRight = 0;
        
        String left = null;
        String right = null;
        
        while (true) {
            
            if (left == null && readLeft < blockSize && leftIt.hasNext()) {
                left = leftIt.next();
                readLeft++;
            }
            
            if (right == null && readRight < blockSize && rightIt.hasNext()) {
                right = rightIt.next();
                readRight++;
            }
            
            if (left == null && right == null) {
                break;
            } else if (left == null) {
                out.writer.write(right);
                right = null;
            } else if (right == null) {
                out.writer.write(left);
                left = null;
            } else if (comparator.compare(left, right) < 0) {
                out.writer.write(left);
                left = null;
            } else {
                out.writer.write(right);
                right = null;
            }
            
            out.writer.newLine();
            out.writer.flush();
            
            result = true;
        }
        
        return result;
    }
    
    private static FileBuffer createBuffer(File dir, String prefix, Charset charset) throws IOException {
        
        FileBuffer buffer = new FileBuffer();
        
        buffer.file = File.createTempFile(prefix, ".tmp", dir);
        buffer.randomAccess = new RandomAccessFile(buffer.file, "rw");
        
        buffer.reader = new BufferedReader(new InputStreamReader(
                new RandomAccessFileInStream(buffer.randomAccess), charset));
        
        buffer.writer = new BufferedWriter(new OutputStreamWriter(
                new RandomAccessFileOutStream(buffer.randomAccess), charset));
        
        return buffer;
    }
    
    private static void closeQuietly(Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (Exception e) {
            // Do nothing
        }
    }
    
    private static void deleteQuietly(File file) {
        try {
            if (file != null) {
                file.delete();
            }
        } catch (Exception e) {
            // Do nothing
        }
    }
}
