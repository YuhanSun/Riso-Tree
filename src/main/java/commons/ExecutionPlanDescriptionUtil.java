package commons;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.ExecutionPlanDescription;


public class ExecutionPlanDescriptionUtil {
  public static enum PlanExecutionType {
    Filter, Expand, NodeByLabelScan, Other,
  }

  public static enum ArgKey {
    ExpandExpression, LabelName,
  }


  public static String[] getEdgeInExpandExpressionPlanNode(
      ExecutionPlanDescription planDescription) {
    Util.println(planDescription);
    Object value = planDescription.getArguments().get(ArgKey.ExpandExpression.name());
    Util.println(value);
    String[] nodeVariables = StringUtils.substringsBetween(value.toString(), "(", ")");
    String nodeVariable1 = nodeVariables[0];
    if (nodeVariable1.contains(":")) {
      nodeVariable1 = StringUtils.split(nodeVariable1, ":")[0];
    }

    String nodeVariable2 = nodeVariables[1];
    if (nodeVariable2.contains(":")) {
      nodeVariable2 = StringUtils.split(nodeVariable2, ":")[0];
    }
    return new String[] {nodeVariable1, nodeVariable2};
  }

  public static PlanExecutionType getPlanExecutionType(String keyword) {
    if (keyword.contains("Expand")) {
      return PlanExecutionType.Expand;
    } else {
      try {
        return PlanExecutionType.valueOf(keyword);
      } catch (Exception e) {
        return PlanExecutionType.Other;
      }
    }
  }

  public static List<ExecutionPlanDescription> getRequired(ExecutionPlanDescription root) {
    List<ExecutionPlanDescription> res = new LinkedList<>();
    Queue<ExecutionPlanDescription> queue = new LinkedList<ExecutionPlanDescription>();
    queue.add(root);
    if (isUseful(root)) {
      res.add(root);
    }
    while (queue.isEmpty() == false) {
      ExecutionPlanDescription planDescription = queue.poll();
      for (ExecutionPlanDescription childPlan : planDescription.getChildren()) {
        queue.add(childPlan);
        if (isUseful(childPlan)) {
          res.add(childPlan);
        }
      }
    }
    return res;
  }

  public static boolean isUseful(ExecutionPlanDescription planDescription) {
    PlanExecutionType type = getPlanExecutionType(planDescription.getName());
    if (type.equals(PlanExecutionType.Expand)) {
      return true;
    }
    return false;
  }
}
