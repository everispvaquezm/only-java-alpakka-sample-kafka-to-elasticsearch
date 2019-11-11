package samples.javadsl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

// Type in Elasticsearch (2)
public class Movement {
    public final int id;
    public final String iban;
    public final Double balance;

    @JsonCreator
    public Movement(@JsonProperty("id") int id,
                    @JsonProperty("iban") String iban,
                    @JsonProperty("balance") Double balance) {
        this.id = id;
        this.iban = iban;
        this.balance = balance;
    }

    @Override
    public String toString() {
        return "Movie(id=" + id + ", iban=" + iban + ", title=" + balance + ")";
    }
}

class JsonMappers {
    // Jackson conversion setup (3)
    public final static ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    public final static ObjectWriter movementWriter = mapper.writerFor(Movement.class);
    public final static ObjectReader movementReader = mapper.readerFor(Movement.class);
}
