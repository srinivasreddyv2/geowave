package mil.nga.giat.geowave.service.rest;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import scala.actors.threadpool.Arrays;

public class SwaggerOperationParser<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			SwaggerOperationParser.class);
	/**
	 * Reads Geowave CLI operations and parses class fields for particular
	 * annotations ( @Parameter and @ParametersDelegate from JCommander) The
	 * parsed data is then used to build up JSON objects that are used by
	 * SwaggerApiParser
	 */

	private final ServiceEnabledCommand<T> operation;
	private JsonObject json_obj = null;

	public JsonObject getJsonObject() {
		return this.json_obj;
	}

	public SwaggerOperationParser(
			final ServiceEnabledCommand<T> op ) {
		this.operation = op;
		this.json_obj = parseParameters();
	}

	public JsonObject[] processField(
			Field f ) {
		Parameter parameter = null;
		try {
			parameter = f.getAnnotation(
					Parameter.class);
		}
		catch (final NoClassDefFoundError e) {
			LOGGER.error(
					"Cannot get parameter",
					e);
		}

		ParametersDelegate parametersDelegate = null;
		try {
			parametersDelegate = f.getAnnotation(
					ParametersDelegate.class);
		}
		catch (final NoClassDefFoundError e) {
			LOGGER.error(
					"Cannot get parameter delegate",
					e);
		}
		if (parameter != null) {
			
		}
		else if (parametersDelegate != null) {
			final List<JsonObject> delegateList = new ArrayList<>();
			for (final Field field : FieldUtils.getAllFields(
					f.getType())) {
				final JsonObject[] fieldArray = processField(
						field);
				if (fieldArray != null) {
					delegateList.addAll(
							Arrays.asList(
									fieldArray));
				}
			}
			return delegateList.toArray(
					new JsonObject[0]);
		}
	}

	public JsonObject[] processField(
			final String name,
			Class<?> type,
			String description,
			boolean required) {

			final JsonObject param_json = new JsonObject();
			boolean mainParam = false;

			// set the "in" type (all query in this case)
			// and also set the type based on the field
			param_json.addProperty(
					"in",
					"query");

			final String swaggerType = JavaToSwaggerType(
					type);
			final String typeInfoForDescription = "";
			if (swaggerType == "array") {
				// handle case for core/main params for a command
				// for now we parse based on assumptions within description
				// TODO see Issue #1185 for details on a more explicit main
				// parameter suggestion
				if (name == "parameters") {
					final String desc = description;
					if (!desc.isEmpty() && desc.matches(
							"(<*>' '*)+")) {
						int currentEndParamIndex = 0;
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
							final String retVal = desc.substring(
									currentStartParamIndex + 1,
									currentEndParamIndex).trim();
							if (!retVal.isEmpty()) {
								if (retVal.startsWith(
										"comma separated list of ")) {
									final JsonObject items_json = new JsonObject();
									items_json.addProperty(
											"type",
											"string");
									param_json.add(
											"items",
											items_json);
								}
								else {

								}
								param_json.addProperty(
										"type",
										String);
								final JsonObject items_json = new JsonObject();
								items_json.addProperty(
										"type",
										"string");
								param_json.add(
										"items",
										items_json);
							}
						}
						while ((currentEndParamIndex > 0) && (currentEndParamIndex < desc.length()));
					}
					else {
						param_json.addProperty(
								"type",
								swaggerType);
						final JsonObject items_json = new JsonObject();
						items_json.addProperty(
								"type",
								"string");
						param_json.add(
								"items",
								items_json);
					}
					mainParam = true;
				}
				else {
					param_json.addProperty(
							"type",
							swaggerType);
					final JsonObject items_json = new JsonObject();
					items_json.addProperty(
							"type",
							JavaToSwaggerType(
									type.getComponentType()));
					param_json.add(
							"items",
							items_json);
				}

			}
			else if (swaggerType == "enum") {
				param_json.addProperty(
						"type",
						"string");
				// The code below is commented out for the time being
				// since most enum fields contain a description that specifies
				// the permitted values
				// if a more automatic approach is desired in the future just
				// uncomment.

				/*
				 * typeInfoForDescription = " ("; for(Object obj:
				 * f.getType().getEnumConstants()) { System.out.println(obj);
				 * typeInfoForDescription += obj.toString() + " "; }
				 * typeInfoForDescription += ")";
				 */
			}
			else {
				param_json.addProperty(
						"type",
						swaggerType);
			}

			// get the description if there is one
			if (!description.isEmpty()) {
				String desc = description + typeInfoForDescription;
				desc = desc.replace(
						"<",
						"[");
				desc = desc.replace(
						">",
						"]");
				param_json.addProperty(
						"description",
						desc);
			}
			// get the field name
			param_json.addProperty(
					"name",
					name);

			// find out if this parameter is required
			param_json.addProperty(
					"required",
					required);

			return new JsonObject[] {
				param_json
			};
	}

	private JsonObject parseParameters() {

		// get the high level attributes from the annotation for the operation
		// (name and description)
		final JsonObject op_json = new JsonObject();
		final String opId = operation.getId();
		op_json.addProperty(
				"operationId",
				opId);

		final Parameters command_annotation = this.operation.getClass().getAnnotation(
				Parameters.class);
		op_json.addProperty(
				"description",
				command_annotation.commandDescription());

		// iterate over the parameters for this operation and add them to the
		// json object
		final JsonArray fields_obj = new JsonArray();
		for (final Field field : FieldUtils.getAllFields(
				this.operation.getClass())) {
			final JsonObject[] field_obj_array = processField(
					field);
			if (field_obj_array != null) {
				for (final JsonObject field_obj : field_obj_array) {
					fields_obj.add(
							field_obj);
				}
			}
		}
		op_json.add(
				"parameters",
				fields_obj);

		// build up the response codes for this operation
		final JsonObject resp_json = new JsonObject();
		JsonObject codes_json = new JsonObject();
		codes_json.addProperty(
				"description",
				"success");
		resp_json.add(
				"200",
				codes_json);

		codes_json = new JsonObject();
		codes_json.addProperty(
				"description",
				"route not found");
		resp_json.add(
				"404",
				codes_json);

		codes_json = new JsonObject();
		codes_json.addProperty(
				"description",
				"invalid or null parameter");
		resp_json.add(
				"500",
				codes_json);

		op_json.add(
				"responses",
				resp_json);

		return op_json;
	}

	private String JavaToSwaggerType(
			final Class<?> type ) {
		// note: array and enum types require deeper handling and
		// thus should be processed outside this method as well
		if (type == String.class) {
			return "string";
		}
		else if ((type == Integer.class) || (type == int.class)) {
			return "integer";
		}
		else if ((type == long.class) || (type == Long.class)) {
			return "long";
		}
		else if ((type == Float.class) || (type == float.class)) {
			return "number";
		}
		else if ((type == Boolean.class) || (type == boolean.class)) {
			return "boolean";
		}
		else if ((type != null) && ((Class<?>) type).isEnum()) {
			return "enum";
		}
		else if ((type == List.class) || ((type != null) && ((Class<?>) type).isArray())) {
			return "array";
		}
		return "string";
	}
}
