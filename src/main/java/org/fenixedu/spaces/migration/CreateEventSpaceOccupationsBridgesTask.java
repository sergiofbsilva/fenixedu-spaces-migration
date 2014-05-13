package org.fenixedu.spaces.migration;

import java.util.Set;

import net.sourceforge.fenixedu.domain.resource.ResourceAllocation;
import net.sourceforge.fenixedu.domain.space.LessonInstanceOccupationBridge;
import net.sourceforge.fenixedu.domain.space.LessonInstanceSpaceOccupation;
import net.sourceforge.fenixedu.domain.space.LessonOccupationBridge;
import net.sourceforge.fenixedu.domain.space.LessonSpaceOccupation;
import net.sourceforge.fenixedu.domain.space.WrittenEvaluationOccupationBridge;
import net.sourceforge.fenixedu.domain.space.WrittenEvaluationSpaceOccupation;

import org.fenixedu.bennu.core.domain.Bennu;
import org.fenixedu.bennu.scheduler.custom.CustomTask;

public class CreateEventSpaceOccupationsBridgesTask extends CustomTask {

    @Override
    public void runTask() throws Exception {
        final Set<ResourceAllocation> resourceAllocationsSet = Bennu.getInstance().getResourceAllocationsSet();
        for (ResourceAllocation resourceAllocation : resourceAllocationsSet) {
            if (resourceAllocation.isEventSpaceOccupation()) {
                if (resourceAllocation.isLessonInstanceSpaceOccupation()) {
                    new LessonInstanceOccupationBridge((LessonInstanceSpaceOccupation) resourceAllocation);
                } else if (resourceAllocation.isLessonSpaceOccupation()) {
                    new LessonOccupationBridge((LessonSpaceOccupation) resourceAllocation);
                } else if (resourceAllocation.isWrittenEvaluationSpaceOccupation()) {
                    new WrittenEvaluationOccupationBridge((WrittenEvaluationSpaceOccupation) resourceAllocation);
                }
            }
        }
    }
}
