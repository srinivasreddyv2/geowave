package mil.nga.giat.geowave.service.rest.field;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.service.rest.GeoWaveOperationServiceWrapper;

public class RestFieldFactory
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			GeoWaveOperationServiceWrapper.class);

	public static List<RestFieldValue<?>> createRestFieldValues(
			final Object instance ) {
		final Class<?> instanceType = instance.getClass();
		final List<RestFieldValue<?>> retVal = new ArrayList<>();
		for (final Field field : FieldUtils.getFieldsWithAnnotation(
				instanceType,
				Parameter.class)) {
			retVal.addAll(
					createRestFieldValues(
							field,
							field.getAnnotation(
									Parameter.class),
							instance));

		}

		for (final Field field : FieldUtils.getFieldsWithAnnotation(
				instanceType,
				ParametersDelegate.class)) {
			try {
				retVal.addAll(
						createRestFieldValues(
								field.getType().newInstance()));
			}
			catch (InstantiationException | IllegalAccessException e) {
				LOGGER.error(
						"Unable to instantiate field",
						e);
			}
		}
		return retVal;
	}

	private static List<RestFieldValue<?>> createRestFieldValues(
			final Field field,
			final Parameter parameter,
			final Object instance ) {
		// handle case for core/main params for a command
		// for now we parse based on assumptions within description
		// TODO see Issue #1185 for details on a more explicit main
		// parameter suggestion
		final String desc = parameter.description();
		if (List.class.isAssignableFrom(
				field.getType()) && !desc.isEmpty()
				&& desc.matches(
						"(<*>' '*)+")) {
			int currentEndParamIndex = 0;
			// this simply is collecting names and a flag to indicate if its a
			// list
			final List<Pair<String, Boolean>> individualParams = new ArrayList<>();
			do {
				final int currentStartParamIndex = desc.indexOf(
						'<',
						currentEndParamIndex);
				if ((currentStartParamIndex < 0) || (currentStartParamIndex >= (desc.length() - 1))) {
					break;
				}
				currentEndParamIndex = desc.indexOf(
						'>',
						currentStartParamIndex + 1);
				final String fullName = desc.substring(
						currentStartParamIndex + 1,
						currentEndParamIndex).trim();
				if (!fullName.isEmpty()) {
					if (fullName.startsWith(
							"comma separated list of ")) {
						individualParams.add(
								ImmutablePair.of(
										fullName.substring(
												24).trim(),
										true));
					}
					else {
						individualParams.add(
								ImmutablePair.of(
										fullName,
										false));
					}
				}
			}
			while ((currentEndParamIndex > 0) && (currentEndParamIndex < desc.length()));

			return Lists.transform(
					individualParams,
					new Function<Pair<String, Boolean>, RestFieldValue<?>>() {
						int i = 0;

						@Override
						public RestFieldValue<?> apply(
								final Pair<String, Boolean> input ) {
							return new MainParamRestFieldValue(
									i++,
									individualParams.size(),
									field,
									new BasicRestField<>(
											input.getLeft(),
											String.class,
											"main parameter",
											true),
									instance);
						}
					});
		}
		else {
			return Collections.singletonList(
					new ParameterRestFieldValue(
							field,
							parameter,
							instance));
		}
	}
}
