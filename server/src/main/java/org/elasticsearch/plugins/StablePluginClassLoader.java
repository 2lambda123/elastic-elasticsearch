/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.io.IOException;
import java.lang.module.ModuleReader;
import java.lang.module.ResolvedModule;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.util.Enumeration;

/**
 * This classloader will load classes from stable plugins.
 * <p>
 * If the stable plugin is modularized, it will be loaded as a module.
 * <p>
 * If the stable plugin is not modularized, a synthetic module will be created
 * for it and all its dependencies.
 * <p>
 * TODO:
 *   * We need a loadClass method
 *   * We need a findResources method
 *   * We need some tests
 *
 * Resources:
 *   * {@link java.lang.ClassLoader}
 *   * {/@link jdk.internal.loader.BuiltinClassLoader} - boot, platform, and app loaders inherit from this
 *   * {/@link jdk.internal.loader.ClassLoaders.BootClassLoader} - see also {/@link jdk.internal.loader.BootLoader}
 *   * {/@link jdk.internal.loader.ClassLoaders.PlatformClassLoader}
 *   * {/@link jdk.internal.loader.ClassLoaders.AppClassLoader}
 *   * {@link java.security.SecureClassLoader} - user-facing classloaders inherit from this
 *   * {@link java.net.URLClassLoader} - loads classes from classpath jars
 *   * {/@link jdk.internal.loader.Loader} - loads classes from modules
 *   * {@link org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader} - one of our custom classloaders
 *   * {@link java.lang.module.ModuleDescriptor} - how you build a module (for an ubermodule)
 *   * <a href="https://github.com/elastic/elasticsearch/pull/88216/files">WIP PR for ubermodule classloader</a>
 *
 * Alternate name ideas
 *   * CrateClassLoader, because we may use the crate metaphor with our plugins
 *   * UberModuleClassLoader, but this only describes one of the code paths
 *   * UberModuleURLClassLoader
 *   * ModularizingClassLoader
 *   * NamedModuleClassLoader, because it will always load into a named module (via module info, or via synthetic module)
 */
public class StablePluginClassLoader extends SecureClassLoader {

    private ResolvedModule module;

    static StablePluginClassLoader getInstance(ClassLoader parent, ResolvedModule module) {
        PrivilegedAction<StablePluginClassLoader> pa = () -> new StablePluginClassLoader(module);
        return AccessController.doPrivileged(pa);
    }
    /**
     * Constructor
     *
     * Do we need an access controller?
     *
     * The constructors for URLClassLoader and jdk.internal.loader.Loader take or construct an object that does the
     * heavy lifting. jdk.internal.loader.Loader takes ResolvedModules for its argument, while URLClassLoader constructs
     * a URLClassPath object. The AppClassLoader, on the other hand, loads modules as it goes.
     *
     * First cut: single jar only, modularized or not
     * Second cut: main jar plus dependencies (can we have modularized main jar with unmodularized dependencies?)
     * Third cut: link it up with the "crate" descriptor
     */
    public StablePluginClassLoader(ResolvedModule module) {
        this.module = module;
    }

    /**
     * @param moduleName
     *         The module name; or {@code null} to find the class in the
     *         {@linkplain #getUnnamedModule() unnamed module} for this
     *         class loader
     * @param name
     *         The <a href="#binary-name">binary name</a> of the class
     *
     * @return
     */
    @Override
    public Class<?> findClass(String moduleName, String name) {
        // built-in classloader:
        // 1. if we have a module name, we get the package name and look up the module,
        //      then load from the module by calling define class with the name and module
        // 2. if there's no moduleName, search on the classpath
        // The system has a map of packages to modules that it gets by loading the module
        // layer -- do we have this for our plugin loading?
        //
        // URLClassLoader does not implement this
        //
        // jdk.internal.loader.Loader can pull a class out of a loaded module pretty easily;
        // ModuleReference does a lot of the work
        return null;
    }

    /**
     * @param name
     *          The <a href="#binary-name">binary name</a> of the class
     *
     * @return
     */
    @Override
    public Class<?> findClass(String name) {
        // built-in classloaders:
        // try to find a module for the class name by looking up package, then do what
        //   findClass(String, String) does
        //
        // URLClassLoader is constructed with a list of classpaths, and searches those classpaths
        // for resources. If it finds a .class file, it calls defineClass
        //
        // jdk.internal.loader.Loader can pull a class out of a loaded module pretty easily;
        // ModuleReference does a lot of the work

        // from jdk.internal.loader.Loader#defineClass
        try (ModuleReader reader = module.reference().open()) {

            String rn = name.replace('.', '/').concat(".class");
            ByteBuffer bb = reader.read(rn).orElse(null);
            if (bb == null) {
                // class not found
                return null;
            }

            try {
                return defineClass(name, bb, (CodeSource) null);
            } finally {
                reader.release(bb);
            }
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * @param name The resource name
     * @return
     */
    @Override
    protected URL findResource(String moduleName, String name) {
        // built-in classloaders:
        // look up module with package name, then find resource on module reference
        // if no module defined, then find on classpath. Finding on classpath involves
        // using URLClassPath, which we can't use because it's in jdk.internal.
        //
        // URLClassLoader does not implement this
        //
        // jdk.internal.loader.Loader uses a ModuleReader for the ModuleReference class,
        // searching the current module, then other modules, and checking access levels
        return null;
    }

    /**
     * @param name The resource name
     * @return
     */
    @Override
    protected URL findResource(String name) {
        // URLClassLoader basically delegates to URLClassPath, which handles access to jars and files
        //
        // jdk.internal.loader.Loader uses a ModuleReader for the ModuleReference class,
        // searching the current module, then other modules, and checking access levels

        try (ModuleReader reader = module.reference().open()) {
            return reader.find(name).orElseThrow().toURL();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
        // URLClassLoader also delegates here
        //
        // jdk.internal.loader.Loader uses a ModuleReader for the ModuleReference class,
        // searching the current module, then other modules, and checking access levels
        return null;
    }
}
