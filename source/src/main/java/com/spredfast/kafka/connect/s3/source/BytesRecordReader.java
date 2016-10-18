package com.spredfast.kafka.connect.s3.source;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.errors.DataException;
import com.amazonaws.services.s3.model.S3Object;

/**
 * Helper for reading raw length encoded records from a chunk file. Not thread safe.
 */
public class BytesRecordReader {

	public static final Pattern DEFAULT_PATTERN = Pattern.compile(
		"(\\/|^)"                        // match the / or the start of the key so we shouldn't have to worry about prefix
			+ "(?<topic>[^/]+?)-"            // assuming no / in topic names
			+ "(?<partition>\\d{5})-"
			+ "(?<offset>\\d{12})\\.gz$"
	);
	private final ByteBuffer lenBuffer = ByteBuffer.allocate(4);

	private final Pattern keyPattern;

	private final boolean includesKeys;

	/**
	 * @param includesKeys do the serialized records include keys? Or just values?
	 */
	public BytesRecordReader(boolean includesKeys) {
		this.includesKeys = includesKeys;
		this.keyPattern = DEFAULT_PATTERN;
	}


	/**
	 * Convenience for reading from an S3 object. Key must match the pattern and data is assumed to be compressed.
	 */
	public Iterable<ConsumerRecord<byte[], byte[]>> readAll(final S3Object object) throws IOException {
		return readAll(object.getKey(), new GetStream() {
			@Override
			public InputStream get() throws IOException {
				return new GZIPInputStream(object.getObjectContent());
			}
		});
	}

	/**
	 * Convenience for reading from a file. Filename must match the key pattern and is assumed to be compressed.
	 */
	public Iterable<ConsumerRecord<byte[], byte[]>> readAll(final File file) throws IOException {
		return readAll(file.getName(), new GetStream() {
			@Override
			public InputStream get() throws IOException {
				return new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)));
			}
		});
	}

	/**
	 * Create an iterator over the records in the given input stream with the given key/filename. Key must
	 * match this pattern: /?[topic-name]-[0 padded partition, width 5]-[0 padded offset, width 12].gz
	 * <p>
	 * Stream must be of decompressed bytes.
	 */
	public Iterator<ConsumerRecord<byte[], byte[]>> readAll(final String key, final InputStream inputStream) throws IOException {
		return readAll(key, new GetStream() {
			@Override
			public InputStream get() throws IOException {
				return inputStream;
			}
		}).iterator();
	}

	protected Iterable<ConsumerRecord<byte[], byte[]>> readAll(String key, final GetStream getStream) {
		final Matcher matcher = keyPattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Not a valid chunk filename! " + key);
		}
		final String topic = matcher.group("topic");
		final int partition = Integer.parseInt(matcher.group("partition"));
		final int offset = Integer.parseInt(matcher.group("offset"));

		return new Iterable<ConsumerRecord<byte[], byte[]>>() {
			@Override
			public Iterator<ConsumerRecord<byte[], byte[]>> iterator() {
				try {
					return BytesRecordReader.this.iterator(topic, partition, offset, getStream.get());
				} catch (IOException e) {
					throw new DataException(e);
				}
			}
		};
	}

	public int partition(String key) {
		final Matcher matcher = keyPattern.matcher(key);
		if (!matcher.find()) {
			throw new IllegalArgumentException("Not a valid chunk filename! " + key);
		}
		return Integer.parseInt(matcher.group("partition"));
	}

	protected interface GetStream {
		InputStream get() throws IOException;
	}

	private Iterator<ConsumerRecord<byte[], byte[]>> iterator(final String topic, final int partition, final int startOffset, final InputStream data) {
		return new Iterator<ConsumerRecord<byte[], byte[]>>() {
			ConsumerRecord<byte[], byte[]> next;

			int offset = startOffset;

			@Override
			public boolean hasNext() {
				try {
					if (next == null) {
						next = read(topic, partition, offset++, data);
					}
				} catch (IOException e) {
					throw new DataException(e);
				}
				return next != null;
			}

			@Override
			public ConsumerRecord<byte[], byte[]> next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				final ConsumerRecord<byte[], byte[]> record = this.next;
				next = null;
				return record;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	/**
	 * Reads a record from the given uncompressed data stream.
	 *
	 * @return a raw ConsumerRecord or null if at the end of the data stream.
	 * @throws IOException
	 */
	public ConsumerRecord<byte[], byte[]> read(String topic, int partition, long offset, InputStream data) throws IOException {
		final byte[] key;
		final int valSize;
		if (includesKeys) {
			// if at the end of the stream, return null
			final Integer keySize = readLen(topic, partition, offset, data);
			if (keySize == null) {
				return null;
			}
			key = readBytes(keySize, data, topic, partition, offset);
			valSize = readValueLen(topic, partition, offset, data);
		} else {
			key = null;
			Integer vSize = readLen(topic, partition, offset, data);
			if (vSize == null) {
				return null;
			}
			valSize = vSize;
		}

		final byte[] value = readBytes(valSize, data, topic, partition, offset);

		return new ConsumerRecord<>(topic, partition, offset, key, value);
	}

	private int readValueLen(String topic, int partition, long offset, InputStream data) throws IOException {
		final Integer len = readLen(topic, partition, offset, data);
		if (len == null) {
			die(topic, partition, offset);
		}
		return len;
	}

	private byte[] readBytes(int keySize, InputStream data, String topic, int partition, long offset) throws IOException {
		final byte[] bytes = new byte[keySize];
		int read = 0;
		while (read < keySize) {
			final int readNow = data.read(bytes, read, keySize - read);
			if (readNow == -1) {
				die(topic, partition, offset);
			}
			read += readNow;
		}
		return bytes;
	}

	private Integer readLen(String topic, int partition, long offset, InputStream data) throws IOException {
		lenBuffer.rewind();
		int read = data.read(lenBuffer.array(), 0, 4);
		if (read == -1) {
			return null;
		} else if (read != 4) {
			die(topic, partition, offset);
		}
		return lenBuffer.getInt();
	}


	protected ConsumerRecord<byte[], byte[]> die(String topic, int partition, long offset) {
		throw new DataException(String.format("Corrupt record at %s-%d:%d", topic, partition, offset));
	}

}
