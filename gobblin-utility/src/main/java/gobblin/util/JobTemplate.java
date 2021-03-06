/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.util;

import com.typesafe.config.Config;
import java.util.Collection;
import java.util.Properties;

/**
 * An interface for claiming methods used for
 * retrieving template configs
 * and properties that are required by user to fit in.
 * <p>
 *   Each data source may have its own Job Template.
 *   Examples are available based on requests.
 * </p>
 *
 */
public interface JobTemplate {

  /**
   * Retrieve all configuration inside pre-written template.
   * @return
   */
  Config getRawTemplateConfig();

  /**
   * Retrieve all configs that are required from user to fill.
   * @return
   */
  Collection<String> getRequiredConfigList();

  /**
   * Return the combine configuration of template and user customized attributes.
   * @return
   */
  Config getResolvedConfig(Properties userProps) ;
}
