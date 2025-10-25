package org.apache.tika.parallel;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentExtractorFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;

/**
 * Factory for creating ParallelEmbeddedDocumentExtractor instances.
 * Used by Tika via tika-config.xml configuration.
 */
public class ParallelEmbeddedDocumentExtractorFactory implements EmbeddedDocumentExtractorFactory {
    
    @Override
    public EmbeddedDocumentExtractor newInstance(Metadata metadata, ParseContext context) {
        return new ParallelEmbeddedDocumentExtractor(context);
    }
}
