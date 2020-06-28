import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StandardProcessor<T,M> extends AbstractProcessor {

	List<Transformation<T,M>> transformations = new ArrayList<>();
	Function<InputStream, Supplier<T>> reader;
	Function<M, byte[]> writer;

	public StandardProcessor(){}

	protected StandardProcessor(Function<InputStream, Supplier<T>> reader, Function<M, byte[]> writer){
		this.reader = reader;
		this.writer = writer;
	}

	public static <T,M> StandardProcessor<T,M> of(Function<InputStream, Supplier<T>> reader, Function<M, byte[]> writer){
		return new StandardProcessor<>(reader, writer);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		LinkedList<FlowFile> flowFiles = new LinkedList<>(session.get(10));
		List<Pipeline> pipelines = transformations.stream().collect(Collectors.groupingBy(Transformation::getRelationship))
				.values().stream()
				.map(transformations -> Pipeline.of(transformations, writer, session.create(), session, getLogger()))
				.collect(Collectors.toList());

		flowFiles.forEach(file -> {
			InputStream inputStream = session.read(file);
			Stream.generate(reader.apply(inputStream))
				.takeWhile(Objects::nonNull)
				.forEach(record -> pipelines.forEach(pipeline -> pipeline.execute(record, file)));
			try {
				inputStream.close();
			} catch (IOException e) {
				getLogger().error("errlllkor");
			}
		});
		pipelines.forEach(Pipeline::close);
		session.remove(flowFiles);
	}

	public StandardProcessor<T, M> withTransformation(Transformation<T,M> transformation){
		transformations.add(transformation);
		return this;
	}

	public StandardProcessor<T, M> withTransformations(List<Transformation<T,M>> transformations){
		this.transformations.addAll(transformations);
		return this;
	}

}
