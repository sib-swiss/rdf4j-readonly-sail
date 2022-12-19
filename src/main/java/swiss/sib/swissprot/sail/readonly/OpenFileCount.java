/*******************************************************************************
 * Copyright (c) 2022 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package swiss.sib.swissprot.sail.readonly;

import java.lang.management.ManagementFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

public class OpenFileCount {

	public static int openFileCount() {
		try {
			MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName = ManagementFactory.getOperatingSystemMXBean().getObjectName();
			MBeanInfo mBeanInfo = platformMBeanServer.getMBeanInfo(objectName);
			for (MBeanAttributeInfo attrInfo : mBeanInfo.getAttributes()) {
				if (attrInfo.getName().contains("OpenFileDescriptorCount")) {
					Object attribute = platformMBeanServer.getAttribute(objectName, attrInfo.getName());
					return Integer.parseInt(attribute.toString());
				}
			}
		} catch (IntrospectionException | InstanceNotFoundException | ReflectionException | AttributeNotFoundException
				| MBeanException e) {
			return 0;
		}
		return 0;
	}

}
