/// ///////////////////////////////////////////////////////////////////////////
// WithOptionalSemanticValidator.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;
import java.util.Set;

public
class OptionalLimitValidator
    implements IValidator<Long>
{
    private Optional<Long> limit;

    public
    OptionalLimitValidator(long limit)
    {
        this.limit = Optional.of(limit);
    }

    public
    OptionalLimitValidator()
    {
        this.limit = Optional.empty();
    }

    @Override
    public ValidationResult<Long>
    validate(Long subject)
    {
        return
            limit
                .map(max -> doValidate(subject,max))
                .orElse(ValidationResult.of(true,subject));
    }

    private ValidationResult<Long>
    doValidate(Long subject,Long max)
    {
        return
            (subject <= max)
                ? ValidationResult.of(true,subject)
                : ValidationResult.of(false,1,subject);
    }

    private void
    writeObject(ObjectOutputStream out)
        throws IOException
    {
        out.writeBoolean(limit.isPresent());

        if (limit.isPresent())
            out.writeLong(limit.get());
    }

    private void
    readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        boolean isPresent = in.readBoolean();

        if (isPresent)
            limit = Optional.of(in.readLong());
        else
            limit = Optional.empty();
    }

}

//////////////////////////////////////////////////////////////////////////////
