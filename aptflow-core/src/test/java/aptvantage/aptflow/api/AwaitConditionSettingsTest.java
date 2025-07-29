package aptvantage.aptflow.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Execution(ExecutionMode.CONCURRENT)
class AwaitConditionSettingsTest {

    StepFunctions mockStepFunctions;

    @BeforeEach
    void setUp() {
        mockStepFunctions = Mockito.mock(StepFunctions.class);
    }

    @Test
    public void stepNameIsRequired() throws Exception {

        // given a new AwaitConditionSettings
        AwaitConditionSettings subject = new AwaitConditionSettings(mockStepFunctions);

        // when awaitCondition is called, then an exception is thrown
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> subject.awaitCondition(() -> true)
        );

        // and the exception message is correct
        assertEquals("stepName must not be null", thrown.getMessage());
    }

    /**
     * Default timeout is 365 Days
     * Default evaluation interval is 1 Minute
     * @throws Exception
     */
    @Test
    public void defaultSettings() throws Exception {

        // given a new AwaitConditionSettings with a name
        AwaitConditionSettings subject = new AwaitConditionSettings(mockStepFunctions)
                .stepName("test");

        // when await is called
        Supplier<Boolean> condition = () -> true;
        subject.awaitCondition(condition);

        // then the settings provided to stepFunction
        Mockito.verify(mockStepFunctions, Mockito.times(1))
                .awaitCondition(subject, condition);

        // and the settings have a timeout of 1 year
        assertEquals(Duration.of(365, ChronoUnit.DAYS), subject.timeout());

        // and the evaluationInterval is 1 minute
        assertEquals(Duration.of(1, ChronoUnit.MINUTES), subject.evaluationInterval());

    }

}