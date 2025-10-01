/// ///////////////////////////////////////////////////////////////////////////
// ValidationResult.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.pipeline.validation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.*;

public
class
ValidationResult<T extends Serializable>
    implements Serializable, Comparable<ValidationResult<T>>
{
    private final boolean      valid;
    private final Set<Integer> codes;
    private final T            subject;

    @JsonCreator
    public
    ValidationResult(
        @JsonProperty("valid")   boolean      valid,
        @JsonProperty("codes")   Set<Integer> codes,
        @JsonProperty("subject") T            subject)
    {
        this.valid = valid;
        this.codes = codes != null ? Set.copyOf(codes) : Set.of();
        this.subject = subject;
    }

    @Override
    public boolean
    equals(Object other)
    {
        return
            other instanceof ValidationResult<?> vr &&
            Objects.equals(isValid(),vr.isValid())&&
            Objects.equals(getCodes(),vr.getCodes()) &&
            Objects.equals(getSubject(),vr.getSubject());
    }

    @Override
    public int
    hashCode()
    {
        return Objects.hash(isValid(),getSubject(),getCodes());
    }

    public boolean
    isValid() { return valid; }

    public Set<Integer>
    getCodes() { return codes; }

    public T
    getSubject() { return subject; }

    @Override
    public int
    compareTo(ValidationResult<T> other)
    {
        int statusComparison =
            Objects.compare(isValid(),other.isValid(),Boolean::compareTo);

        if (statusComparison != 0)
            return statusComparison;

        int codeComparison =
            compareTo(
                getCodes().stream().sorted().toList(),
                other.getCodes().stream().sorted().toList());

        if (codeComparison != 0)
            return codeComparison;

        return
            Objects.compare(
                getSubject().toString(),
                other.getSubject().toString(),
                String::compareTo);
    }

    public static <T extends Serializable> ValidationResult<T>
    of(Boolean valid,T subject)
    {
        return new ValidationResult<>(valid,Set.of(),subject);
    }

    public static <T extends Serializable> ValidationResult<T>
    of(Boolean valid,int code,T subject)
    {
        return new ValidationResult<>(valid,Set.of(code),subject);
    }

    public static <T extends Serializable> ValidationResult<T>
    of(Boolean valid,Set<Integer> codes,T subject)
    {
        return new ValidationResult<>(valid,codes,subject);
    }

    private static int
    compareTo(List<Integer> a,List<Integer> b)
    {
        if (a == null && b == null)
            return 0;

        if (a == null)
            return -1;

        if (b == null)
            return 1;

        int sizeComparison = Integer.compare(a.size(),b.size());

        if (sizeComparison != 0)
            return sizeComparison;

        for (int i = 0; i < a.size(); i++)
        {
            int elementComparison = Integer.compare(a.get(i),b.get(i));

            if (elementComparison != 0)
                return elementComparison;
        }

        return 0;
    }

}

//////////////////////////////////////////////////////////////////////////////

