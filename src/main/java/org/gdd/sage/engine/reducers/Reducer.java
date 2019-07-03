package org.gdd.sage.engine.reducers;

import org.gdd.sage.http.data.SolutionGroup;

import java.util.List;

public interface Reducer {
    void accumulate(SolutionGroup group);

    List<SolutionGroup> getGroups();
}
