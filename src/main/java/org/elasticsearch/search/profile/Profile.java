package org.elasticsearch.search.profile;


import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.search.profile.ProfileQuery;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents the aggregated/collapsed results of a profiled query (e.g. a query wrapped in
 * one or more ProfileQuery queries).  Since queries are not serializable and contain non-profiling
 * components, it needs to be "collapsed" into a common data structure, which the Profile object
 * represents.
 *
 * Profiles may have zero or more children "components", which themselves are Profile objects.  Each
 * Profile objects holds a "time" value which represents the total aggregate time at that level
 * in the tree.
 */
public class Profile implements Streamable, ToXContent {

    // Profiles may have zero or more components (e.g. a profiled Bool may have several components)
    private ArrayList<Profile> components;

    // The short class name (e.g. TermQuery)
    private String className;

    // The Lucene toString() of the class (e.g. "my_field:zach")
    private String details;

    // Aggregate time for this Profile.  Includes timing of children components
    private long time;

    private long totalTime;

    public Profile(ProfileQuery pQuery) {
        this.components = new ArrayList<Profile>();
        components.add(Profile.collapse(pQuery));
    }

    public Profile() {

    }

    public void setClassName(String name) {
        this.className = name;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public void addComponent(Profile child) {
        if (this.components == null) {
            this.components = new ArrayList<Profile>();
        }
        this.components.add(child);
    }

    public void addComponents(ArrayList<Profile> children) {
        if (this.components == null) {
            this.components = children;
        } else {
            this.components.addAll(children);
        }
    }

    public long time() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long totalTime() {
        return totalTime;
    }

    public void setTotalTime(long time) { this.totalTime = time; }

    /**
     * Merge another Profile with this one.  The combined results are merged
     * into this Profile, not `other`
     *
     * @param other Another Profile to merge
     * @return this
     */
    public Profile merge(Profile other) {
        if (components != null && components.size() > 0) {
            for (int i = 0; i < this.components.size(); ++i) {

                if (other.components != null && i < other.components.size()) {
                    components.set(i, components.get(i).merge(other.components.get(i)));
                }
            }
        }
        this.time += other.time();

        return this;
    }


    /**
     * ProfileQueries store their times internally, and nest inside each other like normal queries.
     * To extract/aggregate and serialize this data between shards, we need to collapse it down to a
     * dedicated Profile object
     *
     * @param pQuery A Query that (presumably) contains at least one ProfileQuery
     * @return Returns a Profile object that represents the just the ProfileQuery components of a query
     */
    public static Profile collapse(Query pQuery) {
        ProfileCollapsingVisitor walker = new ProfileCollapsingVisitor();
        return (Profile) walker.apply(pQuery).get(0);
    }

    /**
     * Merge two or more Profile objects into a single Profile.  This combines the timing scores.
     * Profiles *must* have identical structure or else results will be bizarre...possibly throw
     * exceptions too.
     *
     * @param profiles list of profiles to merge
     * @return         Single Profile object representing the merged set
     */
    public static Profile merge(List<Profile> profiles) {

        if (profiles.size() == 0) {
            // TODO ???
        } else if (profiles.size() == 1) {
            profiles.get(0).setTotalTime(profiles.get(0).time());
            return profiles.get(0);
        }

        Profile p = profiles.remove(0);
        for (Profile profile : profiles) {
            p.merge(profile);
        }

        p.setTotalTime(p.time());

        return p;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        builder.field("type", className);
        builder.field("time", time);
        builder.field("relative", String.format("%.5g%%", ((float) time / (float)totalTime)*100f));
        builder.field("lucene", details);

        if (components != null && components.size() > 0) {
            builder.startArray("components");
            for (Profile component : components) {
                component.setTotalTime(totalTime);
                component.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        className = in.readString();
        time = in.readLong();
        totalTime = in.readLong();
        details = in.readString();

        int componentSize = in.readInt();
        components = new ArrayList<Profile>(componentSize);
        for (int i = 0; i < componentSize; ++i) {
            Profile p = new Profile();
            p.readFrom(in);
            components.add(p);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(className);
        out.writeLong(time);
        out.writeLong(totalTime);
        out.writeString(details);
        out.writeInt(components.size());

        if (components != null && components.size() > 0) {
            for (Profile component : components) {
                component.writeTo(out);
            }
        }

    }
}
