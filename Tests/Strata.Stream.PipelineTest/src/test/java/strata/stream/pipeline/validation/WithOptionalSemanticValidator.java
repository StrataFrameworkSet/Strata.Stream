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
class WithOptionalSemanticValidator
    implements ISemanticValidator<Long>
{
    private Optional<Long> limit;

    public
    WithOptionalSemanticValidator(long limit)
    {
        this.limit = Optional.of(limit);
    }

    public
    WithOptionalSemanticValidator()
    {
        this.limit = Optional.empty();
    }

    @Override
    public void
    validate(Long subject)
        throws SemanticValidationFailedException
    {
        limit.ifPresent(
            l ->
                {
                    if (subject > l)
                    {
                        throw new SemanticValidationFailedException(
                            Set.of(1),
                            "Value exceeds limit of " + l);
                    }
                });
    }

    private void
    writeObject(ObjectOutputStream out)
        throws IOException
    {
        //out.defaultWriteObject();
        out.writeBoolean(limit.isPresent());

        if (limit.isPresent())
            out.writeLong(limit.get());
    }

    private void
    readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        //in.defaultReadObject();
        boolean isPresent = in.readBoolean();

        if (isPresent)
            limit = Optional.of(in.readLong());
        else
            limit = Optional.empty();
    }

}

//////////////////////////////////////////////////////////////////////////////
