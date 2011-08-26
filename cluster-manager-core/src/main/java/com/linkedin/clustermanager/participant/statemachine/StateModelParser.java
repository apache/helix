package com.linkedin.clustermanager.participant.statemachine;

import java.lang.reflect.Method;
import java.util.Arrays;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;

/**
 * Finds the method in stateModel to generate
 * 
 * @author kgopalak
 * 
 */
public class StateModelParser
{

	public Method getMethodForTransition(Class<? extends StateModel> clazz,
	    String fromState, String toState, Class<?>[] paramTypes)
	{
		Method method = getMethodForTransitionUsingAnnotation(clazz, fromState,
		    toState, paramTypes);
		if (method == null)
		{
			method = getMethodForTransitionByConvention(clazz, fromState, toState,
			    paramTypes);
		}
		return method;
	}

	/**
	 * This class uses the method naming convention "onBecome" + toState + "From"
	 * + fromState;
	 * 
	 * @param clazz
	 * @param fromState
	 * @param toState
	 * @param paramTypes
	 * @return Method if found else null
	 */
	public Method getMethodForTransitionByConvention(
	    Class<? extends StateModel> clazz, String fromState, String toState,
	    Class<?>[] paramTypes)
	{
		Method methodToInvoke = null;
		String methodName = "onBecome" + toState + "From" + fromState;
		if (fromState.equals("*"))
		{
			methodName = "onBecome" + toState;
		}

		Method[] methods = clazz.getMethods();
		for (Method method : methods)
		{
			if (method.getName().equalsIgnoreCase(methodName))
			{
				Class<?>[] parameterTypes = method.getParameterTypes();
				if (parameterTypes.length == 2
				    && parameterTypes[0].equals(Message.class)
				    && parameterTypes[1].equals(NotificationContext.class))
				{
					methodToInvoke = method;
					break;
				}
			}
		}
		return methodToInvoke;

	}

	/**
	 * This method uses annotations on the StateModel class. Use StateModelInfo
	 * annotation to specify valid states and initial value use Transition to
	 * specify "to" and "from" state
	 * 
	 * @param clazz
	 *          , class which extends StateModel
	 * @param fromState
	 * @param toState
	 * @param paramTypes
	 * @return
	 */
	public Method getMethodForTransitionUsingAnnotation(
	    Class<? extends StateModel> clazz, String fromState, String toState,
	    Class<?>[] paramTypes)
	{
		StateModelInfo stateModelInfo = clazz.getAnnotation(StateModelInfo.class);
		Method methodToInvoke = null;
		if (stateModelInfo != null)
		{
			Method[] methods = clazz.getMethods();
			if (methods != null)
			{
				for (Method method : methods)
				{
					Transition annotation = method.getAnnotation(Transition.class);
					if (annotation != null)
					{
						boolean matchesFrom = annotation.from().equalsIgnoreCase(fromState);
						boolean matchesTo = annotation.to().equalsIgnoreCase(toState);
						boolean matchesParamTypes = Arrays.equals(paramTypes,
						    method.getParameterTypes());
						if (matchesFrom && matchesTo && matchesParamTypes)
						{
							methodToInvoke = method;
							break;
						}
					}
				}
			}
		}

		return methodToInvoke;
	}

	public String getInitialState(Class<? extends StateModel> clazz)
	{
		StateModelInfo stateModelInfo = clazz.getAnnotation(StateModelInfo.class);
		if (stateModelInfo != null)
		{
			return stateModelInfo.initialState();
		}else{
			return StateModel.DEFAULT_INITIAL_STATE;
		}
	}

}
